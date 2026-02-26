// Package mongostore provides a MongoDB-backed implementation of the whatsmeow
// store interfaces, replacing sqlstore.Container for GOWS sessions.
package mongostore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathRand "math/rand"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB collection names — one collection per data type, all scoped to the
// per-session database (one MongoDB database == one WhatsApp session).
const (
	colDevices           = "devices"
	colIdentityKeys      = "identity_keys"
	colSessions          = "sessions"
	colPreKeys           = "prekeys"
	colSenderKeys        = "sender_keys"
	colAppStateSyncKeys  = "app_state_sync_keys"
	colAppStateVersions  = "app_state_versions"
	colAppStateMutations = "app_state_mutations"
	colContacts          = "contacts"
	colChatSettings      = "chat_settings"
	colMsgSecrets        = "message_secrets"
	colPrivacyTokens     = "privacy_tokens"
	colEventBuffer       = "event_buffer"
	colLIDMappings       = "lid_mappings"
	colOutgoingEvents    = "outgoing_events"
)

// ---------------------------------------------------------------------------
// MongoContainer — drop-in replacement for *sqlstore.Container
// ---------------------------------------------------------------------------

// MongoContainer manages a single MongoDB database that stores all
// WhatsApp session data for one GOWS session.
type MongoContainer struct {
	client *mongo.Client
	db     *mongo.Database
	log    waLog.Logger
}

// New connects to MongoDB and returns a MongoContainer.
// The mongoURL must include the database name in the path, e.g.
//
//	mongodb://user:pass@host:27017/waha_gows_mysession
func New(mongoURL string, log waLog.Logger) (*MongoContainer, error) {
	dbName, err := dbNameFromURL(mongoURL)
	if err != nil {
		return nil, fmt.Errorf("mongostore: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(mongoURL)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", err)
	}
	if err = client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongostore: ping: %w", err)
	}

	c := &MongoContainer{
		client: client,
		db:     client.Database(dbName),
		log:    log,
	}
	if err = c.createIndexes(); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongostore: indexes: %w", err)
	}
	return c, nil
}

// Close disconnects from MongoDB.  Satisfies the same contract as
// sqlstore.Container.Close().
func (c *MongoContainer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.client.Disconnect(ctx)
}

// GetFirstDevice loads the persisted device from MongoDB, or returns a freshly
// generated in-memory device if none is stored yet.  The new device is NOT
// written to MongoDB until WhatsApp assigns it a JID (after QR-scan / pair)
// at which point whatsmeow calls PutDevice automatically.
func (c *MongoContainer) GetFirstDevice() (*store.Device, error) {
	device, err := c.loadDevice()
	if err != nil {
		return nil, err
	}
	if device != nil {
		return device, nil
	}
	return c.NewDevice(), nil
}

// NewDevice creates an in-memory store.Device with freshly generated keys.
func (c *MongoContainer) NewDevice() *store.Device {
	device := &store.Device{
		Log:            c.log,
		Container:      c,
		NoiseKey:       keys.NewKeyPair(),
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: mathRand.Uint32(),
		AdvSecretKey:   cryptoRandBytes(32),
	}
	device.SignedPreKey = device.IdentityKey.CreateSignedPreKey(1)
	mds := newMongoDeviceStore(c.db, c.log)
	assignStores(device, mds)
	return device
}

// PutDevice satisfies store.DeviceContainer.
// Called by whatsmeow when the device registers or updates its credentials.
func (c *MongoContainer) PutDevice(ctx context.Context, device *store.Device) error {
	if device.ID == nil {
		return errors.New("mongostore: cannot save device without JID")
	}

	var advDetails, advAccountSig, advAccountSigKey, advDeviceSig []byte
	if device.Account != nil {
		advDetails = device.Account.Details
		advAccountSig = device.Account.AccountSignature
		advAccountSigKey = device.Account.AccountSignatureKey
		advDeviceSig = device.Account.DeviceSignature
	}

	doc := bson.M{
		"_id":                 "device",
		"jid":                 device.ID.String(),
		"registration_id":     device.RegistrationID,
		"noise_key_priv":      device.NoiseKey.Priv[:],
		"identity_key_priv":   device.IdentityKey.Priv[:],
		"signed_pre_key_id":   device.SignedPreKey.KeyID,
		"signed_pre_key_priv": device.SignedPreKey.Priv[:],
		"signed_pre_key_sig":  device.SignedPreKey.Signature[:],
		"adv_secret_key":      device.AdvSecretKey,
		"adv_details":         advDetails,
		"adv_account_sig":     advAccountSig,
		"adv_account_sig_key": advAccountSigKey,
		"adv_device_sig":      advDeviceSig,
		"platform":            device.Platform,
		"business_name":       device.BusinessName,
		"push_name":           device.PushName,
		"facebook_uuid":       device.FacebookUUID.String(),
	}

	ctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	opts := options.Replace().SetUpsert(true)
	_, err := c.db.Collection(colDevices).ReplaceOne(ctx, bson.M{"_id": "device"}, doc, opts)
	return err
}

// DeleteDevice satisfies store.DeviceContainer.
// Called by whatsmeow on logout.
func (c *MongoContainer) DeleteDevice(ctx context.Context, device *store.Device) error {
	ctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	_, err := c.db.Collection(colDevices).DeleteOne(ctx, bson.M{"_id": "device"})
	return err
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (c *MongoContainer) loadDevice() (*store.Device, error) {
	ctx, cancel := dbCtx()
	defer cancel()

	var doc bson.M
	err := c.db.Collection(colDevices).FindOne(ctx, bson.M{"_id": "device"}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: load device: %w", err)
	}

	device := &store.Device{
		Log:          c.log,
		Container:    c,
		SignedPreKey: &keys.PreKey{},
	}

	device.RegistrationID = toUint32(doc["registration_id"])

	noisePriv := toBytes(doc["noise_key_priv"])
	if len(noisePriv) != 32 {
		return nil, fmt.Errorf("mongostore: noise key wrong length %d", len(noisePriv))
	}
	device.NoiseKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(noisePriv))

	identPriv := toBytes(doc["identity_key_priv"])
	if len(identPriv) != 32 {
		return nil, fmt.Errorf("mongostore: identity key wrong length %d", len(identPriv))
	}
	device.IdentityKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(identPriv))

	preKeyPriv := toBytes(doc["signed_pre_key_priv"])
	if len(preKeyPriv) != 32 {
		return nil, fmt.Errorf("mongostore: signed pre-key wrong length %d", len(preKeyPriv))
	}
	preKeySig := toBytes(doc["signed_pre_key_sig"])
	if len(preKeySig) != 64 {
		return nil, fmt.Errorf("mongostore: signed pre-key sig wrong length %d", len(preKeySig))
	}
	device.SignedPreKey.KeyID = toUint32(doc["signed_pre_key_id"])
	device.SignedPreKey.KeyPair = *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(preKeyPriv))
	device.SignedPreKey.Signature = (*[64]byte)(preKeySig)

	device.AdvSecretKey = toBytes(doc["adv_secret_key"])

	if advDetails := toBytes(doc["adv_details"]); len(advDetails) > 0 {
		device.Account = &waProto.ADVSignedDeviceIdentity{
			Details:             advDetails,
			AccountSignature:    toBytes(doc["adv_account_sig"]),
			AccountSignatureKey: toBytes(doc["adv_account_sig_key"]),
			DeviceSignature:     toBytes(doc["adv_device_sig"]),
		}
	}

	device.Platform = toString(doc["platform"])
	device.BusinessName = toString(doc["business_name"])
	device.PushName = toString(doc["push_name"])

	if jidStr := toString(doc["jid"]); jidStr != "" {
		jid, err := types.ParseJID(jidStr)
		if err != nil {
			return nil, fmt.Errorf("mongostore: parse JID %q: %w", jidStr, err)
		}
		device.ID = &jid
	}

	if fbStr := toString(doc["facebook_uuid"]); fbStr != "" {
		device.FacebookUUID, _ = uuid.Parse(fbStr)
	}

	mds := newMongoDeviceStore(c.db, c.log)
	assignStores(device, mds)
	device.Initialized = true
	return device, nil
}

func assignStores(device *store.Device, mds *MongoDeviceStore) {
	adapter := &MongoStoreAdapter{inner: mds}
	device.Identities = adapter
	device.Sessions = adapter
	device.PreKeys = adapter
	device.SenderKeys = adapter
	device.AppStateKeys = adapter
	device.AppState = adapter
	device.Contacts = adapter
	device.ChatSettings = adapter
	device.MsgSecrets = adapter
	device.PrivacyTokens = adapter
	device.EventBuffer = adapter
	device.LIDs = adapter
}

func (c *MongoContainer) createIndexes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type idxSpec struct {
		col  string
		keys bson.D
		name string
	}
	specs := []idxSpec{
		{colIdentityKeys, bson.D{{Key: "address", Value: 1}}, "address_1"},
		{colSessions, bson.D{{Key: "address", Value: 1}}, "address_1"},
		{colPreKeys, bson.D{{Key: "key_id", Value: 1}}, "key_id_1"},
		{colSenderKeys, bson.D{{Key: "group_id", Value: 1}, {Key: "sender_id", Value: 1}}, "group_sender_1"},
		{colAppStateSyncKeys, bson.D{{Key: "key_id_hex", Value: 1}}, "key_id_hex_1"},
		{colAppStateVersions, bson.D{{Key: "name", Value: 1}}, "name_1"},
		{colAppStateMutations, bson.D{{Key: "name", Value: 1}, {Key: "index_mac_hex", Value: 1}}, "name_indexmac_1"},
		{colContacts, bson.D{{Key: "jid", Value: 1}}, "jid_1"},
		{colChatSettings, bson.D{{Key: "jid", Value: 1}}, "jid_1"},
		{colMsgSecrets, bson.D{{Key: "chat_jid", Value: 1}, {Key: "sender_jid", Value: 1}, {Key: "message_id", Value: 1}}, "chat_sender_msg_1"},
		{colPrivacyTokens, bson.D{{Key: "jid", Value: 1}}, "jid_1"},
		{colEventBuffer, bson.D{{Key: "hash_hex", Value: 1}}, "hash_hex_1"},
		{colLIDMappings, bson.D{{Key: "lid_user", Value: 1}}, "lid_user_1"},
		{colLIDMappings, bson.D{{Key: "pn_user", Value: 1}}, "pn_user_1"},
		{colOutgoingEvents, bson.D{{Key: "chat_jid", Value: 1}, {Key: "message_id", Value: 1}}, "chat_msg_1"},
		{colOutgoingEvents, bson.D{{Key: "timestamp", Value: 1}}, "timestamp_1"},
	}

	for _, s := range specs {
		model := mongo.IndexModel{
			Keys:    s.keys,
			Options: options.Index().SetName(s.name),
		}
		if _, err := c.db.Collection(s.col).Indexes().CreateOne(ctx, model); err != nil {
			return fmt.Errorf("index on %s: %w", s.col, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// MongoDeviceStore — implements all whatsmeow store interfaces (no ctx params)
// ---------------------------------------------------------------------------

type MongoDeviceStore struct {
	db  *mongo.Database
	log waLog.Logger

	preKeyMu sync.Mutex

	contactCacheMu sync.Mutex
	contactCache   map[types.JID]types.ContactInfo
}

func newMongoDeviceStore(db *mongo.Database, log waLog.Logger) *MongoDeviceStore {
	return &MongoDeviceStore{
		db:           db,
		log:          log,
		contactCache: make(map[types.JID]types.ContactInfo),
	}
}

// MongoStoreAdapter adapts MongoDeviceStore's legacy method set to the newer
// context-based whatsmeow store interfaces.
type MongoStoreAdapter struct {
	inner *MongoDeviceStore
}

func (a *MongoStoreAdapter) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	return a.inner.PutIdentity(address, key)
}

func (a *MongoStoreAdapter) DeleteAllIdentities(ctx context.Context, phone string) error {
	return a.inner.DeleteAllIdentities(phone)
}

func (a *MongoStoreAdapter) DeleteIdentity(ctx context.Context, address string) error {
	return a.inner.DeleteIdentity(address)
}

func (a *MongoStoreAdapter) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	return a.inner.IsTrustedIdentity(address, key)
}

func (a *MongoStoreAdapter) GetSession(ctx context.Context, address string) ([]byte, error) {
	return a.inner.GetSession(address)
}

func (a *MongoStoreAdapter) HasSession(ctx context.Context, address string) (bool, error) {
	return a.inner.HasSession(address)
}

func (a *MongoStoreAdapter) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(addresses))
	for _, addr := range addresses {
		session, err := a.inner.GetSession(addr)
		if err != nil {
			return nil, err
		}
		result[addr] = session
	}
	return result, nil
}

func (a *MongoStoreAdapter) PutSession(ctx context.Context, address string, session []byte) error {
	return a.inner.PutSession(address, session)
}

func (a *MongoStoreAdapter) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	for addr, session := range sessions {
		if err := a.inner.PutSession(addr, session); err != nil {
			return err
		}
	}
	return nil
}

func (a *MongoStoreAdapter) DeleteAllSessions(ctx context.Context, phone string) error {
	return a.inner.DeleteAllSessions(phone)
}

func (a *MongoStoreAdapter) DeleteSession(ctx context.Context, address string) error {
	return a.inner.DeleteSession(address)
}

func (a *MongoStoreAdapter) MigratePNToLID(ctx context.Context, pn, lid types.JID) error {
	return a.inner.MigratePNToLID(ctx, pn, lid)
}

func (a *MongoStoreAdapter) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	return a.inner.GetOrGenPreKeys(count)
}

func (a *MongoStoreAdapter) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	return a.inner.GenOnePreKey()
}

func (a *MongoStoreAdapter) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	return a.inner.GetPreKey(id)
}

func (a *MongoStoreAdapter) RemovePreKey(ctx context.Context, id uint32) error {
	return a.inner.RemovePreKey(id)
}

func (a *MongoStoreAdapter) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	return a.inner.MarkPreKeysAsUploaded(upToID)
}

func (a *MongoStoreAdapter) UploadedPreKeyCount(ctx context.Context) (int, error) {
	return a.inner.UploadedPreKeyCount()
}

func (a *MongoStoreAdapter) PutSenderKey(ctx context.Context, group, user string, session []byte) error {
	return a.inner.PutSenderKey(group, user, session)
}

func (a *MongoStoreAdapter) GetSenderKey(ctx context.Context, group, user string) ([]byte, error) {
	return a.inner.GetSenderKey(group, user)
}

func (a *MongoStoreAdapter) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	return a.inner.PutAppStateSyncKey(id, key)
}

func (a *MongoStoreAdapter) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	return a.inner.GetAppStateSyncKey(id)
}

func (a *MongoStoreAdapter) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	return a.inner.GetLatestAppStateSyncKeyID()
}

func (a *MongoStoreAdapter) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	cur, err := a.inner.db.Collection(colAppStateSyncKeys).Find(
		dbctx,
		bson.M{},
		options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(dbctx)
	var out []*store.AppStateSyncKey
	for cur.Next(dbctx) {
		var doc bson.M
		if err = cur.Decode(&doc); err != nil {
			return nil, err
		}
		key := &store.AppStateSyncKey{
			Data:        toBytes(doc["key_data"]),
			Fingerprint: toBytes(doc["fingerprint"]),
			Timestamp:   toInt64(doc["timestamp"]),
		}
		if len(key.Data) > 0 {
			out = append(out, key)
		}
	}
	return out, cur.Err()
}

func (a *MongoStoreAdapter) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	return a.inner.PutAppStateVersion(name, version, hash)
}

func (a *MongoStoreAdapter) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return a.inner.GetAppStateVersion(name)
}

func (a *MongoStoreAdapter) DeleteAppStateVersion(ctx context.Context, name string) error {
	return a.inner.DeleteAppStateVersion(name)
}

func (a *MongoStoreAdapter) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	return a.inner.PutAppStateMutationMACs(name, version, mutations)
}

func (a *MongoStoreAdapter) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	return a.inner.DeleteAppStateMutationMACs(name, indexMACs)
}

func (a *MongoStoreAdapter) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	return a.inner.GetAppStateMutationMAC(name, indexMAC)
}

func (a *MongoStoreAdapter) PutPushName(ctx context.Context, user types.JID, pushName string) (bool, string, error) {
	return a.inner.PutPushName(user, pushName)
}

func (a *MongoStoreAdapter) PutBusinessName(ctx context.Context, user types.JID, businessName string) (bool, string, error) {
	return a.inner.PutBusinessName(user, businessName)
}

func (a *MongoStoreAdapter) PutContactName(ctx context.Context, user types.JID, fullName, firstName string) error {
	return a.inner.PutContactName(user, fullName, firstName)
}

func (a *MongoStoreAdapter) PutAllContactNames(ctx context.Context, contacts []store.ContactEntry) error {
	return a.inner.PutAllContactNames(contacts)
}

func (a *MongoStoreAdapter) PutManyRedactedPhones(ctx context.Context, entries []store.RedactedPhoneEntry) error {
	if len(entries) == 0 {
		return nil
	}
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	for _, entry := range entries {
		jid := entry.JID.ToNonAD()
		_, err := a.inner.db.Collection(colContacts).UpdateOne(
			dbctx,
			bson.M{"jid": jid.String()},
			bson.M{
				"$set":         bson.M{"redacted_phone": entry.RedactedPhone},
				"$setOnInsert": bson.M{"jid": jid.String()},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return err
		}
		a.inner.contactCacheMu.Lock()
		info := a.inner.contactCache[jid]
		info.RedactedPhone = entry.RedactedPhone
		a.inner.contactCache[jid] = info
		a.inner.contactCacheMu.Unlock()
	}
	return nil
}

func (a *MongoStoreAdapter) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	return a.inner.GetContact(user.ToNonAD())
}

func (a *MongoStoreAdapter) GetAllContacts(ctx context.Context) (map[types.JID]types.ContactInfo, error) {
	return a.inner.GetAllContacts()
}

func (a *MongoStoreAdapter) PutMutedUntil(ctx context.Context, chat types.JID, mutedUntil time.Time) error {
	return a.inner.PutMutedUntil(chat, mutedUntil)
}

func (a *MongoStoreAdapter) PutPinned(ctx context.Context, chat types.JID, pinned bool) error {
	return a.inner.PutPinned(chat, pinned)
}

func (a *MongoStoreAdapter) PutArchived(ctx context.Context, chat types.JID, archived bool) error {
	return a.inner.PutArchived(chat, archived)
}

func (a *MongoStoreAdapter) GetChatSettings(ctx context.Context, chat types.JID) (types.LocalChatSettings, error) {
	return a.inner.GetChatSettings(chat)
}

func (a *MongoStoreAdapter) PutMessageSecrets(ctx context.Context, inserts []store.MessageSecretInsert) error {
	return a.inner.PutMessageSecrets(inserts)
}

func (a *MongoStoreAdapter) PutMessageSecret(ctx context.Context, chat, sender types.JID, id types.MessageID, secret []byte) error {
	return a.inner.PutMessageSecret(chat, sender, id, secret)
}

func (a *MongoStoreAdapter) GetMessageSecret(ctx context.Context, chat, sender types.JID, id types.MessageID) ([]byte, types.JID, error) {
	secret, err := a.inner.GetMessageSecret(chat, sender, id)
	if err != nil || secret == nil {
		return secret, types.EmptyJID, err
	}
	return secret, sender.ToNonAD(), nil
}

func (a *MongoStoreAdapter) PutPrivacyTokens(ctx context.Context, tokens ...store.PrivacyToken) error {
	return a.inner.PutPrivacyTokens(tokens...)
}

func (a *MongoStoreAdapter) GetPrivacyToken(ctx context.Context, user types.JID) (*store.PrivacyToken, error) {
	return a.inner.GetPrivacyToken(user.ToNonAD())
}

func (a *MongoStoreAdapter) GetBufferedEvent(ctx context.Context, ciphertextHash [32]byte) (*store.BufferedEvent, error) {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	hashHex := hex.EncodeToString(ciphertextHash[:])
	var doc bson.M
	err := a.inner.db.Collection(colEventBuffer).FindOne(dbctx, bson.M{"hash_hex": hashHex}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &store.BufferedEvent{
		Plaintext:  toBytes(doc["plaintext"]),
		ServerTime: time.Unix(toInt64(doc["server_timestamp"]), 0),
		InsertTime: time.UnixMilli(toInt64(doc["insert_timestamp"])),
	}, nil
}

func (a *MongoStoreAdapter) PutBufferedEvent(ctx context.Context, ciphertextHash [32]byte, plaintext []byte, serverTimestamp time.Time) error {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	hashHex := hex.EncodeToString(ciphertextHash[:])
	doc := bson.M{
		"hash_hex":         hashHex,
		"plaintext":        plaintext,
		"server_timestamp": serverTimestamp.Unix(),
		"insert_timestamp": time.Now().UnixMilli(),
	}
	_, err := a.inner.db.Collection(colEventBuffer).ReplaceOne(
		dbctx,
		bson.M{"hash_hex": hashHex},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (a *MongoStoreAdapter) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return fn(ctx)
}

func (a *MongoStoreAdapter) ClearBufferedEventPlaintext(ctx context.Context, ciphertextHash [32]byte) error {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	hashHex := hex.EncodeToString(ciphertextHash[:])
	_, err := a.inner.db.Collection(colEventBuffer).UpdateOne(
		dbctx,
		bson.M{"hash_hex": hashHex},
		bson.M{"$set": bson.M{"plaintext": nil}},
	)
	return err
}

func (a *MongoStoreAdapter) DeleteOldBufferedHashes(ctx context.Context) error {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	cutoff := time.Now().Add(-14 * 24 * time.Hour).UnixMilli()
	_, err := a.inner.db.Collection(colEventBuffer).DeleteMany(
		dbctx,
		bson.M{"insert_timestamp": bson.M{"$lt": cutoff}},
	)
	return err
}

func (a *MongoStoreAdapter) GetOutgoingEvent(ctx context.Context, chatJID, altChatJID types.JID, id types.MessageID) (string, []byte, error) {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	var doc bson.M
	err := a.inner.db.Collection(colOutgoingEvents).FindOne(
		dbctx,
		bson.M{
			"chat_jid":   bson.M{"$in": []string{chatJID.ToNonAD().String(), altChatJID.ToNonAD().String()}},
			"message_id": string(id),
			"timestamp":  bson.M{"$exists": true},
			"plaintext":  bson.M{"$exists": true},
			"format":     bson.M{"$exists": true},
		},
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	return toString(doc["format"]), toBytes(doc["plaintext"]), nil
}

func (a *MongoStoreAdapter) AddOutgoingEvent(ctx context.Context, chatJID types.JID, id types.MessageID, format string, plaintext []byte) error {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	doc := bson.M{
		"chat_jid":   chatJID.ToNonAD().String(),
		"message_id": string(id),
		"format":     format,
		"plaintext":  plaintext,
		"timestamp":  time.Now().UnixMilli(),
	}
	_, err := a.inner.db.Collection(colOutgoingEvents).ReplaceOne(
		dbctx,
		bson.M{"chat_jid": chatJID.ToNonAD().String(), "message_id": string(id)},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (a *MongoStoreAdapter) DeleteOldOutgoingEvents(ctx context.Context) error {
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	cutoff := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	_, err := a.inner.db.Collection(colOutgoingEvents).DeleteMany(
		dbctx,
		bson.M{"timestamp": bson.M{"$lt": cutoff}},
	)
	return err
}

func (a *MongoStoreAdapter) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	for _, m := range mappings {
		if err := a.PutLIDMapping(ctx, m.LID, m.PN); err != nil {
			return err
		}
	}
	return nil
}

func (a *MongoStoreAdapter) PutLIDMapping(ctx context.Context, lid, pn types.JID) error {
	lid = lid.ToNonAD()
	pn = pn.ToNonAD()
	if lid.Server != types.HiddenUserServer || pn.Server != types.DefaultUserServer {
		return fmt.Errorf("invalid PutLIDMapping call %s/%s", lid, pn)
	}
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	_, err := a.inner.db.Collection(colLIDMappings).DeleteMany(
		dbctx,
		bson.M{"pn_user": pn.User, "lid_user": bson.M{"$ne": lid.User}},
	)
	if err != nil {
		return err
	}
	_, err = a.inner.db.Collection(colLIDMappings).ReplaceOne(
		dbctx,
		bson.M{"lid_user": lid.User},
		bson.M{"lid_user": lid.User, "pn_user": pn.User},
		options.Replace().SetUpsert(true),
	)
	return err
}

func (a *MongoStoreAdapter) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	lid = lid.ToNonAD()
	if lid.Server != types.HiddenUserServer {
		return types.JID{}, fmt.Errorf("invalid GetPNForLID call with non-LID JID %s", lid)
	}
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	var doc bson.M
	err := a.inner.db.Collection(colLIDMappings).FindOne(dbctx, bson.M{"lid_user": lid.User}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return types.JID{}, nil
	}
	if err != nil {
		return types.JID{}, err
	}
	return types.NewJID(toString(doc["pn_user"]), types.DefaultUserServer), nil
}

func (a *MongoStoreAdapter) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	pn = pn.ToNonAD()
	if pn.Server != types.DefaultUserServer {
		return types.JID{}, fmt.Errorf("invalid GetLIDForPN call with non-PN JID %s", pn)
	}
	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	var doc bson.M
	err := a.inner.db.Collection(colLIDMappings).FindOne(dbctx, bson.M{"pn_user": pn.User}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return types.JID{}, nil
	}
	if err != nil {
		return types.JID{}, err
	}
	return types.NewJID(toString(doc["lid_user"]), types.HiddenUserServer), nil
}

func (a *MongoStoreAdapter) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	result := make(map[types.JID]types.JID)
	if len(pns) == 0 {
		return result, nil
	}

	pnUsers := make([]string, 0, len(pns))
	pnByUser := make(map[string][]types.JID)
	for _, pn := range pns {
		pn = pn.ToNonAD()
		if pn.Server != types.DefaultUserServer {
			continue
		}
		if _, ok := pnByUser[pn.User]; !ok {
			pnUsers = append(pnUsers, pn.User)
		}
		pnByUser[pn.User] = append(pnByUser[pn.User], pn)
	}
	if len(pnUsers) == 0 {
		return result, nil
	}

	dbctx, cancel := dbCtxFrom(ctx)
	defer cancel()
	cur, err := a.inner.db.Collection(colLIDMappings).Find(
		dbctx,
		bson.M{"pn_user": bson.M{"$in": pnUsers}},
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(dbctx)

	for cur.Next(dbctx) {
		var doc bson.M
		if err = cur.Decode(&doc); err != nil {
			return nil, err
		}
		pnUser := toString(doc["pn_user"])
		lidUser := toString(doc["lid_user"])
		if pnUser == "" || lidUser == "" {
			continue
		}
		for _, pn := range pnByUser[pnUser] {
			result[pn] = types.NewJID(lidUser, types.HiddenUserServer)
		}
	}
	return result, cur.Err()
}

// ---------------------------------------------------------------------------
// IdentityStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutIdentity(address string, key [32]byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	doc := bson.M{"address": address, "identity": key[:]}
	opts := options.Replace().SetUpsert(true)
	_, err := m.db.Collection(colIdentityKeys).ReplaceOne(ctx, bson.M{"address": address}, doc, opts)
	return err
}

func (m *MongoDeviceStore) DeleteAllIdentities(phone string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colIdentityKeys).DeleteMany(ctx,
		bson.M{"address": bson.M{"$regex": "^" + phone + "\\."}})
	return err
}

func (m *MongoDeviceStore) DeleteIdentity(address string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colIdentityKeys).DeleteOne(ctx, bson.M{"address": address})
	return err
}

func (m *MongoDeviceStore) IsTrustedIdentity(address string, key [32]byte) (bool, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colIdentityKeys).FindOne(ctx, bson.M{"address": address}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true, nil // Trust on first use
	}
	if err != nil {
		return false, err
	}
	stored := toBytes(doc["identity"])
	if len(stored) != 32 {
		return false, nil
	}
	return *(*[32]byte)(stored) == key, nil
}

// ---------------------------------------------------------------------------
// SessionStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) GetSession(address string) ([]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colSessions).FindOne(ctx, bson.M{"address": address}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toBytes(doc["session"]), nil
}

func (m *MongoDeviceStore) HasSession(address string) (bool, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	count, err := m.db.Collection(colSessions).CountDocuments(ctx, bson.M{"address": address})
	return count > 0, err
}

func (m *MongoDeviceStore) PutSession(address string, session []byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	doc := bson.M{"address": address, "session": session}
	opts := options.Replace().SetUpsert(true)
	_, err := m.db.Collection(colSessions).ReplaceOne(ctx, bson.M{"address": address}, doc, opts)
	return err
}

func (m *MongoDeviceStore) DeleteAllSessions(phone string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colSessions).DeleteMany(ctx,
		bson.M{"address": bson.M{"$regex": "^" + phone + "\\."}})
	return err
}

func (m *MongoDeviceStore) DeleteSession(address string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colSessions).DeleteOne(ctx, bson.M{"address": address})
	return err
}

func (m *MongoDeviceStore) MigratePNToLID(ctx context.Context, pn, lid types.JID) error {
	pnSignal := pn.SignalAddressUser()
	lidSignal := lid.SignalAddressUser()
	if pnSignal == "" || lidSignal == "" || pnSignal == lidSignal {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sessionsUpdated, err := m.migrateSignalAddressField(ctx, colSessions, "address", pnSignal, lidSignal, "address")
	if err != nil {
		return fmt.Errorf("migrate sessions: %w", err)
	}
	identityKeysUpdated, err := m.migrateSignalAddressField(ctx, colIdentityKeys, "address", pnSignal, lidSignal, "address")
	if err != nil {
		return fmt.Errorf("migrate identity keys: %w", err)
	}
	senderKeysUpdated, err := m.migrateSignalAddressField(ctx, colSenderKeys, "sender_id", pnSignal, lidSignal, "group_id", "sender_id")
	if err != nil {
		return fmt.Errorf("migrate sender keys: %w", err)
	}

	if sessionsUpdated > 0 || identityKeysUpdated > 0 || senderKeysUpdated > 0 {
		m.log.Infof("Migrated %d sessions, %d identity keys and %d sender keys from %s to %s", sessionsUpdated, identityKeysUpdated, senderKeysUpdated, pnSignal, lidSignal)
	} else {
		m.log.Debugf("No sessions or sender keys found to migrate from %s to %s", pnSignal, lidSignal)
	}
	return nil
}

func (m *MongoDeviceStore) migrateSignalAddressField(ctx context.Context, collectionName, field, pnSignal, lidSignal string, uniqueFields ...string) (int64, error) {
	regex := "^" + regexp.QuoteMeta(pnSignal) + "[:.]"
	filter := bson.M{field: bson.M{"$regex": regex}}

	cur, err := m.db.Collection(collectionName).Find(ctx, filter)
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)

	var docs []bson.M
	if err = cur.All(ctx, &docs); err != nil {
		return 0, err
	}

	var updated int64
	for _, doc := range docs {
		oldID := toString(doc[field])
		if oldID == "" {
			continue
		}

		newID := replaceSignalAddressPrefix(oldID, pnSignal, lidSignal)
		if newID == "" || newID == oldID {
			continue
		}

		nextDoc := bson.M{}
		for k, v := range doc {
			nextDoc[k] = v
		}
		nextDoc[field] = newID

		nextFilter, err := filterFromFields(nextDoc, uniqueFields)
		if err != nil {
			return updated, err
		}
		_, err = m.db.Collection(collectionName).ReplaceOne(
			ctx,
			nextFilter,
			nextDoc,
			options.Replace().SetUpsert(true),
		)
		if err != nil {
			return updated, err
		}

		oldFilter, err := filterFromFields(doc, uniqueFields)
		if err != nil {
			return updated, err
		}
		_, err = m.db.Collection(collectionName).DeleteOne(ctx, oldFilter)
		if err != nil {
			return updated, err
		}
		updated++
	}

	return updated, nil
}

func replaceSignalAddressPrefix(value, from, to string) string {
	for _, delim := range []string{":", "."} {
		prefix := from + delim
		if strings.HasPrefix(value, prefix) {
			return to + value[len(from):]
		}
	}
	return ""
}

func filterFromFields(doc bson.M, fields []string) (bson.M, error) {
	filter := bson.M{}
	for _, field := range fields {
		value, ok := doc[field]
		if !ok {
			return nil, fmt.Errorf("missing field %q in document", field)
		}
		filter[field] = value
	}
	return filter, nil
}

// ---------------------------------------------------------------------------
// PreKeyStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) GetOrGenPreKeys(count uint32) ([]*keys.PreKey, error) {
	m.preKeyMu.Lock()
	defer m.preKeyMu.Unlock()

	ctx, cancel := dbCtx()
	defer cancel()

	// How many un-uploaded prekeys do we already have?
	existing, err := m.db.Collection(colPreKeys).CountDocuments(ctx, bson.M{"uploaded": false})
	if err != nil {
		return nil, err
	}

	toGen := int(count) - int(existing)
	if toGen > 0 {
		// Find the highest key_id so we can continue numbering
		startID := uint32(1)
		var maxDoc bson.M
		findOpts := options.FindOne().SetSort(bson.D{{Key: "key_id", Value: -1}})
		if e := m.db.Collection(colPreKeys).FindOne(ctx, bson.M{}, findOpts).Decode(&maxDoc); e == nil {
			startID = toUint32(maxDoc["key_id"]) + 1
		}
		docs := make([]interface{}, toGen)
		for i := 0; i < toGen; i++ {
			kp := keys.NewKeyPair()
			docs[i] = bson.M{
				"key_id":   startID + uint32(i),
				"key_pub":  kp.Pub[:],
				"key_priv": kp.Priv[:],
				"uploaded": false,
			}
		}
		if _, err = m.db.Collection(colPreKeys).InsertMany(ctx, docs); err != nil {
			return nil, err
		}
	}

	cur, err := m.db.Collection(colPreKeys).Find(ctx,
		bson.M{"uploaded": false},
		options.Find().SetLimit(int64(count)).SetSort(bson.D{{Key: "key_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var result []*keys.PreKey
	for cur.Next(ctx) {
		var doc bson.M
		if err = cur.Decode(&doc); err != nil {
			return nil, err
		}
		pk, err := docToPreKey(doc)
		if err != nil {
			return nil, err
		}
		result = append(result, pk)
	}
	return result, cur.Err()
}

func (m *MongoDeviceStore) GenOnePreKey() (*keys.PreKey, error) {
	pks, err := m.GetOrGenPreKeys(1)
	if err != nil {
		return nil, err
	}
	if len(pks) == 0 {
		return nil, errors.New("mongostore: GenOnePreKey returned nothing")
	}
	return pks[0], nil
}

func (m *MongoDeviceStore) GetPreKey(id uint32) (*keys.PreKey, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colPreKeys).FindOne(ctx, bson.M{"key_id": id}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToPreKey(doc)
}

func (m *MongoDeviceStore) RemovePreKey(id uint32) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colPreKeys).DeleteOne(ctx, bson.M{"key_id": id})
	return err
}

func (m *MongoDeviceStore) MarkPreKeysAsUploaded(upToID uint32) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colPreKeys).UpdateMany(ctx,
		bson.M{"key_id": bson.M{"$lte": upToID}},
		bson.M{"$set": bson.M{"uploaded": true}})
	return err
}

func (m *MongoDeviceStore) UploadedPreKeyCount() (int, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	count, err := m.db.Collection(colPreKeys).CountDocuments(ctx, bson.M{"uploaded": true})
	return int(count), err
}

// ---------------------------------------------------------------------------
// SenderKeyStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutSenderKey(group, user string, session []byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	filter := bson.M{"group_id": group, "sender_id": user}
	doc := bson.M{"group_id": group, "sender_id": user, "key": session}
	opts := options.Replace().SetUpsert(true)
	_, err := m.db.Collection(colSenderKeys).ReplaceOne(ctx, filter, doc, opts)
	return err
}

func (m *MongoDeviceStore) GetSenderKey(group, user string) ([]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colSenderKeys).FindOne(ctx,
		bson.M{"group_id": group, "sender_id": user}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toBytes(doc["key"]), nil
}

// ---------------------------------------------------------------------------
// AppStateSyncKeyStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutAppStateSyncKey(id []byte, key store.AppStateSyncKey) error {
	ctx, cancel := dbCtx()
	defer cancel()
	idHex := hex.EncodeToString(id)
	doc := bson.M{
		"key_id_hex":  idHex,
		"key_data":    key.Data,
		"fingerprint": key.Fingerprint,
		"timestamp":   key.Timestamp,
	}
	opts := options.Replace().SetUpsert(true)
	_, err := m.db.Collection(colAppStateSyncKeys).ReplaceOne(ctx,
		bson.M{"key_id_hex": idHex}, doc, opts)
	return err
}

func (m *MongoDeviceStore) GetAppStateSyncKey(id []byte) (*store.AppStateSyncKey, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colAppStateSyncKeys).FindOne(ctx,
		bson.M{"key_id_hex": hex.EncodeToString(id)}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &store.AppStateSyncKey{
		Data:        toBytes(doc["key_data"]),
		Fingerprint: toBytes(doc["fingerprint"]),
		Timestamp:   toInt64(doc["timestamp"]),
	}, nil
}

func (m *MongoDeviceStore) GetLatestAppStateSyncKeyID() ([]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})
	err := m.db.Collection(colAppStateSyncKeys).FindOne(ctx, bson.M{}, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(toString(doc["key_id_hex"]))
}

// ---------------------------------------------------------------------------
// AppStateStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutAppStateVersion(name string, version uint64, hash [128]byte) error {
	ctx, cancel := dbCtx()
	defer cancel()
	doc := bson.M{"name": name, "version": int64(version), "hash": hash[:]}
	opts := options.Replace().SetUpsert(true)
	_, err := m.db.Collection(colAppStateVersions).ReplaceOne(ctx,
		bson.M{"name": name}, doc, opts)
	return err
}

func (m *MongoDeviceStore) GetAppStateVersion(name string) (uint64, [128]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colAppStateVersions).FindOne(ctx, bson.M{"name": name}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, [128]byte{}, nil
	}
	if err != nil {
		return 0, [128]byte{}, err
	}
	var hash [128]byte
	if b := toBytes(doc["hash"]); len(b) == 128 {
		copy(hash[:], b)
	}
	return uint64(toInt64(doc["version"])), hash, nil
}

func (m *MongoDeviceStore) DeleteAppStateVersion(name string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colAppStateVersions).DeleteOne(ctx, bson.M{"name": name})
	return err
}

func (m *MongoDeviceStore) PutAppStateMutationMACs(name string, version uint64, mutations []store.AppStateMutationMAC) error {
	if len(mutations) == 0 {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	docs := make([]interface{}, len(mutations))
	for i, mut := range mutations {
		docs[i] = bson.M{
			"name":          name,
			"version":       int64(version),
			"index_mac_hex": hex.EncodeToString(mut.IndexMAC),
			"value_mac":     mut.ValueMAC,
		}
	}
	_, err := m.db.Collection(colAppStateMutations).InsertMany(ctx, docs)
	return err
}

func (m *MongoDeviceStore) DeleteAppStateMutationMACs(name string, indexMACs [][]byte) error {
	if len(indexMACs) == 0 {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	hexes := make([]string, len(indexMACs))
	for i, mac := range indexMACs {
		hexes[i] = hex.EncodeToString(mac)
	}
	_, err := m.db.Collection(colAppStateMutations).DeleteMany(ctx,
		bson.M{"name": name, "index_mac_hex": bson.M{"$in": hexes}})
	return err
}

func (m *MongoDeviceStore) GetAppStateMutationMAC(name string, indexMAC []byte) ([]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	var doc bson.M
	err := m.db.Collection(colAppStateMutations).FindOne(ctx,
		bson.M{"name": name, "index_mac_hex": hex.EncodeToString(indexMAC)}, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toBytes(doc["value_mac"]), nil
}

// ---------------------------------------------------------------------------
// ContactStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutPushName(user types.JID, pushName string) (bool, string, error) {
	m.contactCacheMu.Lock()
	defer m.contactCacheMu.Unlock()
	old := m.contactCache[user].PushName
	if old == pushName {
		return false, old, nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colContacts).UpdateOne(ctx,
		bson.M{"jid": user.String()},
		bson.M{"$set": bson.M{"push_name": pushName}, "$setOnInsert": bson.M{"jid": user.String()}},
		options.Update().SetUpsert(true))
	if err != nil {
		return false, old, err
	}
	info := m.contactCache[user]
	info.PushName = pushName
	m.contactCache[user] = info
	return true, old, nil
}

func (m *MongoDeviceStore) PutBusinessName(user types.JID, businessName string) (bool, string, error) {
	m.contactCacheMu.Lock()
	defer m.contactCacheMu.Unlock()
	old := m.contactCache[user].BusinessName
	if old == businessName {
		return false, old, nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colContacts).UpdateOne(ctx,
		bson.M{"jid": user.String()},
		bson.M{"$set": bson.M{"business_name": businessName}, "$setOnInsert": bson.M{"jid": user.String()}},
		options.Update().SetUpsert(true))
	if err != nil {
		return false, old, err
	}
	info := m.contactCache[user]
	info.BusinessName = businessName
	m.contactCache[user] = info
	return true, old, nil
}

func (m *MongoDeviceStore) PutContactName(user types.JID, fullName, firstName string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colContacts).UpdateOne(ctx,
		bson.M{"jid": user.String()},
		bson.M{
			"$set":         bson.M{"full_name": fullName, "first_name": firstName},
			"$setOnInsert": bson.M{"jid": user.String()},
		},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	m.contactCacheMu.Lock()
	info := m.contactCache[user]
	info.FullName = fullName
	info.FirstName = firstName
	m.contactCache[user] = info
	m.contactCacheMu.Unlock()
	return nil
}

func (m *MongoDeviceStore) PutAllContactNames(contacts []store.ContactEntry) error {
	if len(contacts) == 0 {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	m.contactCacheMu.Lock()
	defer m.contactCacheMu.Unlock()
	for _, c := range contacts {
		jidStr := c.JID.String()
		_, err := m.db.Collection(colContacts).UpdateOne(ctx,
			bson.M{"jid": jidStr},
			bson.M{
				"$set":         bson.M{"full_name": c.FullName, "first_name": c.FirstName},
				"$setOnInsert": bson.M{"jid": jidStr},
			},
			options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
		info := m.contactCache[c.JID]
		info.FullName = c.FullName
		info.FirstName = c.FirstName
		m.contactCache[c.JID] = info
	}
	return nil
}

func (m *MongoDeviceStore) GetContact(user types.JID) (types.ContactInfo, error) {
	m.contactCacheMu.Lock()
	if info, ok := m.contactCache[user]; ok {
		m.contactCacheMu.Unlock()
		return info, nil
	}
	m.contactCacheMu.Unlock()

	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colContacts).FindOne(ctx, bson.M{"jid": user.String()}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return types.ContactInfo{}, nil
	}
	if err != nil {
		return types.ContactInfo{}, err
	}
	info := types.ContactInfo{
		PushName:     toString(doc["push_name"]),
		BusinessName: toString(doc["business_name"]),
		FullName:     toString(doc["full_name"]),
		FirstName:    toString(doc["first_name"]),
	}
	m.contactCacheMu.Lock()
	m.contactCache[user] = info
	m.contactCacheMu.Unlock()
	return info, nil
}

func (m *MongoDeviceStore) GetAllContacts() (map[types.JID]types.ContactInfo, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	cur, err := m.db.Collection(colContacts).Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	result := make(map[types.JID]types.ContactInfo)
	for cur.Next(ctx) {
		var doc bson.M
		if err = cur.Decode(&doc); err != nil {
			return nil, err
		}
		jid, err := types.ParseJID(toString(doc["jid"]))
		if err != nil {
			continue
		}
		result[jid] = types.ContactInfo{
			PushName:     toString(doc["push_name"]),
			BusinessName: toString(doc["business_name"]),
			FullName:     toString(doc["full_name"]),
			FirstName:    toString(doc["first_name"]),
		}
	}
	return result, cur.Err()
}

// ---------------------------------------------------------------------------
// ChatSettingsStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutMutedUntil(chat types.JID, mutedUntil time.Time) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colChatSettings).UpdateOne(ctx,
		bson.M{"jid": chat.String()},
		bson.M{
			"$set":         bson.M{"muted_until": mutedUntil.Unix()},
			"$setOnInsert": bson.M{"jid": chat.String()},
		},
		options.Update().SetUpsert(true))
	return err
}

func (m *MongoDeviceStore) PutPinned(chat types.JID, pinned bool) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colChatSettings).UpdateOne(ctx,
		bson.M{"jid": chat.String()},
		bson.M{
			"$set":         bson.M{"pinned": pinned},
			"$setOnInsert": bson.M{"jid": chat.String()},
		},
		options.Update().SetUpsert(true))
	return err
}

func (m *MongoDeviceStore) PutArchived(chat types.JID, archived bool) error {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := m.db.Collection(colChatSettings).UpdateOne(ctx,
		bson.M{"jid": chat.String()},
		bson.M{
			"$set":         bson.M{"archived": archived},
			"$setOnInsert": bson.M{"jid": chat.String()},
		},
		options.Update().SetUpsert(true))
	return err
}

func (m *MongoDeviceStore) GetChatSettings(chat types.JID) (types.LocalChatSettings, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colChatSettings).FindOne(ctx, bson.M{"jid": chat.String()}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return types.LocalChatSettings{}, nil
	}
	if err != nil {
		return types.LocalChatSettings{}, err
	}
	return types.LocalChatSettings{
		Found:      true,
		MutedUntil: time.Unix(toInt64(doc["muted_until"]), 0),
		Pinned:     toBool(doc["pinned"]),
		Archived:   toBool(doc["archived"]),
	}, nil
}

// ---------------------------------------------------------------------------
// MsgSecretStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutMessageSecrets(inserts []store.MessageSecretInsert) error {
	if len(inserts) == 0 {
		return nil
	}
	ctx, cancel := dbCtx()
	defer cancel()
	docs := make([]interface{}, len(inserts))
	for i, ins := range inserts {
		docs[i] = bson.M{
			"chat_jid":   ins.Chat.String(),
			"sender_jid": ins.Sender.String(),
			"message_id": string(ins.ID),
			"secret":     ins.Secret,
		}
	}
	_, err := m.db.Collection(colMsgSecrets).InsertMany(ctx, docs,
		options.InsertMany().SetOrdered(false))
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (m *MongoDeviceStore) PutMessageSecret(chat, sender types.JID, id types.MessageID, secret []byte) error {
	return m.PutMessageSecrets([]store.MessageSecretInsert{
		{Chat: chat, Sender: sender, ID: id, Secret: secret},
	})
}

func (m *MongoDeviceStore) GetMessageSecret(chat, sender types.JID, id types.MessageID) ([]byte, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colMsgSecrets).FindOne(ctx, bson.M{
		"chat_jid":   chat.String(),
		"sender_jid": sender.String(),
		"message_id": string(id),
	}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toBytes(doc["secret"]), nil
}

// ---------------------------------------------------------------------------
// PrivacyTokenStore
// ---------------------------------------------------------------------------

func (m *MongoDeviceStore) PutPrivacyTokens(tokens ...store.PrivacyToken) error {
	ctx, cancel := dbCtx()
	defer cancel()
	for _, token := range tokens {
		jidStr := token.User.String()
		_, err := m.db.Collection(colPrivacyTokens).UpdateOne(ctx,
			bson.M{"jid": jidStr},
			bson.M{
				"$set":         bson.M{"token": token.Token, "timestamp": token.Timestamp.Unix()},
				"$setOnInsert": bson.M{"jid": jidStr},
			},
			options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MongoDeviceStore) GetPrivacyToken(user types.JID) (*store.PrivacyToken, error) {
	ctx, cancel := dbCtx()
	defer cancel()
	var doc bson.M
	err := m.db.Collection(colPrivacyTokens).FindOne(ctx,
		bson.M{"jid": user.String()}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &store.PrivacyToken{
		User:      user,
		Token:     toBytes(doc["token"]),
		Timestamp: time.Unix(toInt64(doc["timestamp"]), 0),
	}, nil
}

// ---------------------------------------------------------------------------
// Shared utilities
// ---------------------------------------------------------------------------

// dbCtx returns a context suitable for a single database call.
func dbCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func dbCtxFrom(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, 10*time.Second)
}

func dbNameFromURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid MongoDB URL: %w", err)
	}
	dbName := strings.TrimPrefix(parsed.Path, "/")
	dbName = strings.SplitN(dbName, "/", 2)[0] // only the first path segment
	if dbName == "" {
		return "", errors.New("no database name in MongoDB URL — append /<dbname> to the URL, e.g. mongodb://host:27017/waha_gows_mysession")
	}
	return dbName, nil
}

func docToPreKey(doc bson.M) (*keys.PreKey, error) {
	priv := toBytes(doc["key_priv"])
	if len(priv) != 32 {
		return nil, fmt.Errorf("mongostore: prekey priv wrong length %d", len(priv))
	}
	pk := &keys.PreKey{
		KeyID:   toUint32(doc["key_id"]),
		KeyPair: *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(priv)),
	}
	return pk, nil
}

func cryptoRandBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("mongostore: crypto/rand.Read: %v", err))
	}
	return b
}

func toBytes(v interface{}) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case primitive.Binary:
		return val.Data
	}
	return nil
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toUint32(v interface{}) uint32 {
	switch val := v.(type) {
	case int32:
		return uint32(val)
	case int64:
		return uint32(val)
	case float64:
		return uint32(val)
	case int:
		return uint32(val)
	case uint32:
		return val
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case int:
		return int64(val)
	}
	return 0
}

func toBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
