/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.metastore;

import static com.mongodb.client.model.Filters.*;
import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoDbMetaStore implements MetaStore {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStore.class);

  private static final String META_STORE_DB = "gungnir_metastore";
  private static final String USER_ACCOUNT_COLLECTION = "account";
  private static final String SCHEMA_COLLECTION = "schema";
  private static final String TOPOLOGY_COLLECTION = "topology";
  private static final String TRACKING_COLLECTION = "tracking";

  private MongoClient mongoClient;
  private MongoDatabase metaStoreDB;
  private MongoCollection<Document> userAccountCollection;
  private MongoCollection<Document> schemaCollection;
  private MongoCollection<Document> topologyCollection;
  private MongoCollection<Document> trackingCollection;
  private Map<String, Integer> trackingMap;

  private void createIndexUserAccount() throws MetaStoreException {
    try {
      userAccountCollection.createIndex(new Document("name", 1), new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to ensure index of user account collection", e);
      throw new MetaStoreException("Failed to ensure index user account collection", e);
    }
  }

  private void createIndexSchema() throws MetaStoreException {
    try {
      schemaCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to ensure index of schema collection", e);
      throw new MetaStoreException("Failed to ensure index schema collection", e);
    }
  }

  private void createIndexTopology() throws MetaStoreException {
    try {
      topologyCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to ensure index of topology collection", e);
      throw new MetaStoreException("Failed to ensure index topology collection", e);
    }
  }

  @Override
  public void open() throws MetaStoreException {
    GungnirConfig config = GungnirManager.getManager().getConfig();
    List<String> servers = config.getList(METASTORE_MONGODB_SERVERS);
    List<ServerAddress> addresses = Lists.newArrayListWithCapacity(servers.size());
    for (String server : servers) {
      addresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(addresses);
    metaStoreDB = mongoClient.getDatabase(META_STORE_DB);
    userAccountCollection = metaStoreDB.getCollection(USER_ACCOUNT_COLLECTION);
    schemaCollection = metaStoreDB.getCollection(SCHEMA_COLLECTION);
    topologyCollection = metaStoreDB.getCollection(TOPOLOGY_COLLECTION);
    trackingCollection = metaStoreDB.getCollection(TRACKING_COLLECTION);

    trackingMap = Maps.newConcurrentMap();
  }

  @Override
  public void init() throws MetaStoreException {
    createIndexUserAccount();
    createIndexSchema();
    createIndexTopology();

    UserEntity rootUser = null;
    while (rootUser == null) {
      try {
        rootUser = findUserAccountByName(ROOT_USER_NAME);
      } catch (NotStoredException e) {
        try {
          rootUser = new UserEntity(ROOT_USER_NAME);
          rootUser.setPassword(ROOT_USER_PASSWORD);
          insertUserAccount(rootUser);
        } catch (AlreadyStoredException ignore) {
          ignore = null;
        }
      }
    }

    try {
      createTrackingNoSequence();
    } catch (AlreadyStoredException ignore) {
      ignore = null;
    }
  }

  @Override
  public void drop() throws MetaStoreException {
    try {
      metaStoreDB.drop();
    } catch (MongoException e) {
      LOG.error("Failed to drop metastore database", e);
      throw new MetaStoreException("Failed to drop metastore database", e);
    }
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  @Override
  public void insertUserAccount(UserEntity user) throws MetaStoreException,
      AlreadyStoredException {
    try {
      Date createTime = new Date();
      Document doc = new Document("name", user.getName()).append("password", user.getPassword())
          .append("createTime", createTime);
      userAccountCollection.insertOne(doc);

      user.setId(doc.getObjectId("_id").toString());
      user.setCreateTime(createTime);

      LOG.info("Successful to insert user account '{}'", user.getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        throw new AlreadyStoredException("'" + user.getName() + "' already exists");
      } else {
        LOG.error("Failed to insert user account", e);
        throw new MetaStoreException("Failed to insert user account", e);
      }
    }
  }

  public UserEntity findUserAccountById(String accountId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(accountId)) {
      throw new MetaStoreException("Invalid account ID '" + accountId + "'");
    }

    try {
      Document doc = userAccountCollection.find(eq("_id", new ObjectId(accountId))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find user account '" + accountId + "'");
      }
      UserEntity user = new UserEntity();
      user.setId(doc.getObjectId("_id").toString());
      user.setName(doc.getString("name"));
      user.setPassword(doc.get("password", Binary.class).getData());
      user.setCreateTime(doc.getDate("createTime"));
      user.setLastModifyTime(doc.getDate("lastModifyTime"));
      return user;
    } catch (MongoException e) {
      LOG.error("Failed to find user account", e);
      throw new MetaStoreException("Failed to find user account", e);
    }
  }

  @Override
  public UserEntity findUserAccountByName(String userName) throws MetaStoreException,
      NotStoredException {
    try {
      Document doc = userAccountCollection.find(eq("name", userName)).first();
      if (doc == null) {
        throw new NotStoredException("Can't find user account '" + userName + "'");
      }
      UserEntity user = new UserEntity();
      user.setId(doc.getObjectId("_id").toString());
      user.setName(doc.getString("name"));
      user.setPassword(doc.get("password", Binary.class).getData());
      user.setCreateTime(doc.getDate("createTime"));
      user.setLastModifyTime(doc.getDate("lastModifyTime"));
      return user;
    } catch (MongoException e) {
      LOG.error("Failed to find user account", e);
      throw new MetaStoreException("Failed to find user account", e);
    }
  }

  @Override
  public List<UserEntity> findUserAccounts() throws MetaStoreException {
    try {
      List<UserEntity> users = Lists.newArrayList();

      MongoCursor<Document> cursor = userAccountCollection.find().iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          UserEntity user = new UserEntity();
          user.setId(doc.getObjectId("_id").toString());
          user.setName(doc.getString("name"));
          user.setPassword(doc.get("password", Binary.class).getData());
          user.setCreateTime(doc.getDate("createTime"));
          user.setLastModifyTime(doc.getDate("lastModifyTime"));

          users.add(user);
        }
      } finally {
        cursor.close();
      }

      return users;
    } catch (MongoException e) {
      LOG.error("Failed to find user accounts", e);
      throw new MetaStoreException("Failed to find user accounts", e);
    }
  }

  @Override
  public void updateUserAccount(UserEntity user) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      Date lastModifyTime = new Date();
      UpdateResult result = userAccountCollection.updateOne(eq("_id", new ObjectId(user.getId())),
          new Document("$set", new Document("name", user.getName())
              .append("password", user.getPassword()).append("lastModifyTime", lastModifyTime)));

      if (result.getModifiedCount() > 0) {
        user.setLastModifyTime(lastModifyTime);

        LOG.info("Successful to update user account '{}'", user.getName());
      } else {
        throw new NotStoredException("Can't find user account '" + user.getId() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to update user account", e);
      throw new MetaStoreException("Failed to update user account", e);
    }
  }

  @Override
  public void changeUserAccountPassword(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      Date lastModifyTime = new Date();
      UpdateResult result = userAccountCollection.updateOne(eq("_id", new ObjectId(user.getId())),
          new Document("$set", new Document("password", user.getPassword())
              .append("lastModifyTime", lastModifyTime)));

      if (result.getModifiedCount() > 0) {
        user.setLastModifyTime(lastModifyTime);

        LOG.info("Successful to change password '{}'", user.getName());
      } else {
        throw new NotStoredException("Can't find user account '" + user.getId() + "'");
      }

      LOG.info("Successful to change password '{}'", user.getName());
    } catch (MongoException e) {
      LOG.error("Failed to change password", e);
      throw new MetaStoreException("Failed to change password", e);
    }
  }

  @Override
  public void deleteUserAccount(UserEntity user) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      userAccountCollection.deleteOne(eq("_id", new ObjectId(user.getId())));

      LOG.info("Successful to delete user account '{}'", user.getName());
    } catch (MongoException e) {
      LOG.error("Failed to delete user account", e);
      throw new MetaStoreException("Failed to delete user account", e);
    }
  }

  @Override
  public void insertSchema(Schema schema) throws MetaStoreException, AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc = new Document("name", schema.getSchemaName())
          .append("topologies", schema.getTopologies()).append("desc", Utils.serialize(schema))
          .append("owner", schema.getOwner().getId()).append("createTime", createTime);
      if (schema.getComment() != null) {
        doc.append("comment", schema.getComment());
      }
      schemaCollection.insertOne(doc);

      schema.setId(doc.getObjectId("_id").toString());
      schema.setCreateTime(createTime);

      LOG.info("Successful to insert schema {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        throw new AlreadyStoredException(schema.getSchemaName() + " already exists");
      } else {
        LOG.error("Failed to insert schema", e);
        throw new MetaStoreException("Failed to insert schema", e);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Schema findSchema(String schemaName, UserEntity owner) throws MetaStoreException,
      NotStoredException {
    try {
      Document doc = schemaCollection.find(and(eq("name", schemaName), eq("owner", owner.getId())))
          .first();
      if (doc == null) {
        throw new NotStoredException(schemaName + " isn't registered");
      }

      Schema schema = (Schema) Utils.deserialize(doc.get("desc", Binary.class).getData());
      schema.setId(doc.getObjectId("_id").toString());
      schema.setTopologies(doc.get("topologies", ArrayList.class));
      schema.setOwner(owner);
      schema.setCreateTime(doc.getDate("createTime"));
      schema.setComment(doc.getString("comment"));
      return schema;
    } catch (MongoException e) {
      LOG.error("Failed to find schema", e);
      throw new MetaStoreException("Failed to find schema", e);
    }
  }

  @Override
  public boolean changeSchemasToUsed(Schema schema, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(schema.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + schema.getId() + "'");
    }

    try {
      UpdateResult result = schemaCollection.updateOne(and(eq("_id", new ObjectId(schema.getId())),
          eq("createTime", schema.getCreateTime())),
          new Document("$push", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change to busy schema {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change to free schema {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName(), e);
      throw new MetaStoreException("Failed to busy schemas", e);
    }
  }

  @Override
  public boolean changeSchemasToFree(Schema schema, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(schema.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + schema.getId() + "'");
    }

    try {
      UpdateResult result = schemaCollection.updateOne(and(eq("_id", new ObjectId(schema.getId())),
          eq("createTime", schema.getCreateTime())),
          new Document("$pull", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change to free schema {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change to free schema {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName(), e);
      throw new MetaStoreException("Failed to find schemas", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Schema> findSchemas(UserEntity owner) throws MetaStoreException {
    try {
      List<Schema> schemas = Lists.newArrayList();

      MongoCursor<Document> cursor = schemaCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();

          Schema schema = (Schema) Utils.deserialize(doc.get("desc", Binary.class).getData());
          schema.setId(doc.getObjectId("_id").toString());
          schema.setTopologies(doc.get("topologies", ArrayList.class));
          schema.setOwner(owner);
          schema.setCreateTime(doc.getDate("createTime"));
          schema.setComment(doc.getString("comment"));

          schemas.add(schema);
        }
      } finally {
        cursor.close();
      }

      return schemas;
    } catch (MongoException e) {
      LOG.error("Failed to find schemas", e);
      throw new MetaStoreException("Failed to find schemas", e);
    }
  }

  @Override
  public boolean deleteSchema(Schema schema) throws MetaStoreException, NotStoredException {
    try {
      DeleteResult result = schemaCollection.deleteOne(and(eq("name", schema.getSchemaName()),
          eq("owner", schema.getOwner().getId()), eq("topologies", new String[0])));

      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete schema {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete schema", e);
      throw new MetaStoreException("Failed to delete schema", e);
    }
  }

  @Override
  public void insertTopology(GungnirTopology topology) throws MetaStoreException,
      AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc = new Document("name", topology.getName())
          .append("status", topology.getStatus().toString())
          .append("desc", Utils.serialize(topology)).append("owner", topology.getOwner().getId())
          .append("createTime", createTime);
      if (topology.getComment() != null) {
        doc.append("comment", topology.getComment());
      }
      topologyCollection.insertOne(doc);

      topology.setId(doc.getObjectId("_id").toString());
      topology.setCreateTime(createTime);

      LOG.info("Successful to insert topology '{}'", topology.getId());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        throw new AlreadyStoredException(topology.getName() + " already exists");
      } else {
        LOG.error("Failed to insert topology", e);
        throw new MetaStoreException("Failed to insert topology", e);
      }
    }
  }

  @Override
  public boolean changeTopologyStatus(GungnirTopology topology) throws MetaStoreException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
      TopologyStatus status = null;
      switch (topology.getStatus()) {
        case STARTING:
          status = TopologyStatus.STOPPED;
          break;
        case RUNNING:
          status = TopologyStatus.STARTING;
          break;
        case STOPPING:
          status = TopologyStatus.RUNNING;
          break;
        case STOPPED:
          status = TopologyStatus.STOPPING;
          break;
        default:
          return false;
      }

      UpdateResult result = topologyCollection.updateOne(
          and(eq("_id", new ObjectId(topology.getId())), eq("status", status.toString())),
          new Document("$set", new Document("status", topology.getStatus().toString())));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change topology status '{}' ({}->{})", topology.getId(), status,
            topology.getStatus());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change topology status", e);
      throw new MetaStoreException("Failed to change topology status", e);
    }
  }

  @Override
  public void changeForcedTopologyStatus(GungnirTopology topology) throws MetaStoreException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
      topologyCollection.updateOne(eq("_id", new ObjectId(topology.getId())),
          new Document("$set", new Document("status", topology.getStatus().toString())));
      LOG.info("Successful to change topology status '{}' ({})", topology.getId(),
          topology.getStatus());
    } catch (MongoException e) {
      LOG.error("Failed to change topology status", e);
      throw new MetaStoreException("Failed to change topology status", e);
    }
  }

  @Override
  public List<GungnirTopology> findTopologies(UserEntity owner) throws MetaStoreException {
    try {
      List<GungnirTopology> topologies = Lists.newArrayList();

      MongoCursor<Document> cursor = topologyCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          GungnirTopology topology = (GungnirTopology) Utils.deserialize(
              doc.get("desc", Binary.class).getData());
          topology.setId(doc.getObjectId("_id").toString());
          topology.setName(doc.getString("name"));
          topology.setOwner(owner);
          topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
          topology.setCreateTime(doc.getDate("createTime"));
          topology.setComment(doc.getString("comment"));

          topologies.add(topology);
        }
      } finally {
        cursor.close();
      }

      return topologies;
    } catch (MongoException e) {
      LOG.error("Failed to find topologies", e);
      throw new MetaStoreException("Failed to find topologies", e);
    }
  }

  @Override
  public List<GungnirTopology> findTopologies(UserEntity owner, TopologyStatus status)
      throws MetaStoreException {
    try {
      List<GungnirTopology> topologies = Lists.newArrayList();

      MongoCursor<Document> cursor = topologyCollection.find(
          and(eq("owner", owner.getId()), eq("status", status.toString()))).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          GungnirTopology topology = (GungnirTopology) Utils.deserialize(
              doc.get("desc", Binary.class).getData());
          topology.setId(doc.getObjectId("_id").toString());
          topology.setName(doc.getString("name"));
          topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
          topology.setOwner(owner);
          topology.setCreateTime(doc.getDate("createTime"));
          topology.setComment(doc.getString("comment"));

          topologies.add(topology);
        }
      } finally {
        cursor.close();
      }

      return topologies;
    } catch (MongoException e) {
      LOG.error("Failed to find topologies", e);
      throw new MetaStoreException("Failed to find topologies", e);
    }
  }

  @Override
  public GungnirTopology findTopologyById(String topologyId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(topologyId)) {
      throw new MetaStoreException("Invalid topology ID '" + topologyId + "'");
    }

    try {
      Document doc = topologyCollection.find(eq("_id", new ObjectId(topologyId))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find topology '" + topologyId + "'");
      }

      GungnirTopology topology = (GungnirTopology) Utils.deserialize(
          doc.get("desc", Binary.class).getData());
      topology.setId(doc.getObjectId("_id").toString());
      topology.setName(doc.getString("name"));
      topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
      topology.setOwner(findUserAccountById(doc.getString("owner")));
      topology.setCreateTime(doc.getDate("createTime"));
      topology.setComment(doc.getString("comment"));
      return topology;
    } catch (MongoException e) {
      LOG.error("Failed to find topology", e);
      throw new MetaStoreException("Failed to find topology", e);
    }
  }

  @Override
  public GungnirTopology findTopologyByName(String topologyName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    try {
      Document doc = topologyCollection.find(
          and(eq("name", topologyName), eq("owner", owner.getId()))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find topology '" + topologyName + "'");
      }

      GungnirTopology topology = (GungnirTopology) Utils.deserialize(
          doc.get("desc", Binary.class).getData());
      topology.setId(doc.getObjectId("_id").toString());
      topology.setName(doc.getString("name"));
      topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
      topology.setOwner(owner);
      topology.setCreateTime(doc.getDate("createTime"));
      topology.setComment(doc.getString("comment"));
      return topology;
    } catch (MongoException e) {
      LOG.error("Failed to find topology", e);
      throw new MetaStoreException("Failed to find topology", e);
    }
  }

  @Override
  public TopologyStatus getTopologyStatus(String topologyId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(topologyId)) {
      throw new MetaStoreException("Invalid topology ID '" + topologyId + "'");
    }

    try {
      Document doc = topologyCollection.find(
          eq("_id", new ObjectId(topologyId))).projection(new Document("status", 1)).first();
      if (doc == null) {
        throw new NotStoredException("Can't find topology '" + topologyId + "'");
      }
      return TopologyStatus.valueOf(doc.getString("status"));
    } catch (MongoException e) {
      LOG.error("Failed to get topology status", e);
      throw new MetaStoreException("Failed to get topology status", e);
    }
  }

  @Override
  public void deleteTopology(GungnirTopology topology) throws MetaStoreException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
      topologyCollection.deleteOne(eq("_id", new ObjectId(topology.getId())));

      LOG.info("Successful to delete topology {} owned by {}", topology.getId(),
          topology.getOwner().getName());
    } catch (MongoException e) {
      LOG.error("Failed to delete topology", e);
      throw new MetaStoreException("Failed to delete topology", e);
    }
  }

  private void createTrackingNoSequence() throws MetaStoreException, AlreadyStoredException {
    try {
      trackingCollection.insertOne(new Document("_id", "_tno").append("sequence", 0));
      LOG.info("Successful to create tracking no sequence");
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        throw new AlreadyStoredException("Tracking no sequence already exists");
      } else {
        LOG.error("Failed to create tracking no sequence", e);
        throw new MetaStoreException("Failed to create tracking no sequence", e);
      }
    }
  }

  @Override
  public String generateTrackingId() throws MetaStoreException {
    try {
      Document seq = trackingCollection.findOneAndUpdate(eq("_id", "_tno"),
          new Document("$inc", new Document("sequence", 1)));
      Integer tno = seq.getInteger("sequence");
      Date createTime = new Date();
      Document doc = new Document("no", tno).append("createTime", createTime);
      trackingCollection.insertOne(doc);
      String tid = doc.getObjectId("_id").toString();

      trackingMap.put(tid, tno);

      LOG.info("Successful to generate tracking ID '{}', tracking No {}", tid, tno);
      return tid;
    } catch (MongoException e) {
      LOG.error("Failed to generate UUID", e);
      throw new MetaStoreException("Failed to generate tracking ID", e);
    }
  }

  @Override
  public Integer getTrackingNo(String tid) throws MetaStoreException, NotStoredException {
    Integer tno = trackingMap.get(tid);
    if (tno != null) {
      return tno;
    }

    if (!ObjectId.isValid(tid)) {
      throw new MetaStoreException("Invalid tracking ID '" + tid + "'");
    }

    try {
      Document doc = trackingCollection.find(eq("_id", new ObjectId(tid))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find tracking ID '" + tid + "'");
      }

      tno = doc.getInteger("no");
      trackingMap.put(tid, tno);
      return tno;
    } catch (MongoException e) {
      LOG.error("Failed to find tracking ID", e);
      throw new MetaStoreException("Failed to find tracking ID", e);
    }
  }
}
