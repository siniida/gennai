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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class InMemoryMetaStore implements MetaStore {

  private static Logger LOG = LoggerFactory.getLogger(InMemoryMetaStore.class);

  private Map<String, UserEntity> usersById = Maps.newHashMap();
  private Map<String, UserEntity> usersByName = Maps.newHashMap();
  private Map<String, Map<String, Schema>> schemasByName = Maps.newHashMap();
  private Map<String, GungnirTopology> topologiesById = Maps.newHashMap();
  private Map<String, Map<String, GungnirTopology>> topologiesByName = Maps.newHashMap();
  private Map<String, Integer> trackingIds = Maps.newHashMap();
  private int currentTrackingId;

  @Override
  public synchronized void open() throws MetaStoreException {
  }

  @Override
  public synchronized void init() throws MetaStoreException {
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
  }

  @Override
  public synchronized void drop() throws MetaStoreException {
    usersById = Maps.newHashMap();
    usersByName = Maps.newHashMap();
    schemasByName = Maps.newHashMap();
    topologiesById = Maps.newHashMap();
    topologiesByName = Maps.newHashMap();
    trackingIds = Maps.newHashMap();
    currentTrackingId = 0;
  }

  @Override
  public synchronized void close() {
  }

  @Override
  public synchronized void insertUserAccount(UserEntity user) throws MetaStoreException,
      AlreadyStoredException {
    if (!usersByName.containsKey(user.getName())) {
      user.setId(generateUniqueId());
      user.setCreateTime(new Date());
      UserEntity userCopy = user.clone();
      usersById.put(user.getId(), userCopy);
      usersByName.put(user.getName(), userCopy);
      LOG.info("Added user account: " + user.getName());
    } else {
      throw new AlreadyStoredException("'" + user.getName() + "' already exists");
    }
  }

  @Override
  public synchronized UserEntity findUserAccountById(String accountId)
      throws MetaStoreException, NotStoredException {
    if (!usersById.containsKey(accountId)) {
      throw new NotStoredException("Can't find user account '" + accountId + "'");
    } else {
      return usersById.get(accountId).clone();
    }
  }

  @Override
  public synchronized UserEntity findUserAccountByName(String userName)
      throws MetaStoreException, NotStoredException {
    if (!usersByName.containsKey(userName)) {
      throw new NotStoredException("Can't find user account '" + userName + "'");
    } else {
      return usersByName.get(userName).clone();
    }
  }

  @Override
  public synchronized List<UserEntity> findUserAccounts() throws MetaStoreException {
    List<UserEntity> results = Lists.newArrayList();
    for (UserEntity user : usersById.values()) {
      results.add(user.clone());
    }
    return results;
  }

  @Override
  public synchronized void updateUserAccount(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (usersById.containsKey(user.getId())) {
      user.setLastModifyTime(new Date());

      UserEntity storedUser = usersById.get(user.getId());
      usersByName.remove(storedUser.getName());

      storedUser.setName(user.getName());
      storedUser.setPassword(user.getPassword());
      storedUser.setLastModifyTime(user.getLastModifyTime());

      usersByName.put(storedUser.getName(), storedUser);

      LOG.info("Update user account: " + user.getName());
    } else {
      throw new NotStoredException("Can't find user account '" + user.getId() + "'");
    }
  }

  @Override
  public synchronized void changeUserAccountPassword(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (usersById.containsKey(user.getId())) {
      user.setLastModifyTime(new Date());
      UserEntity storedUser = usersById.get(user.getId());
      storedUser.setPassword(user.getPassword());
      storedUser.setLastModifyTime(user.getLastModifyTime());
      LOG.info("Change password: " + user.getName());
    } else {
      throw new NotStoredException("Can't find user account '" + user.getId() + "'");
    }
  }

  @Override
  public synchronized void deleteUserAccount(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (!usersById.containsKey(user.getId())) {
      throw new NotStoredException("Can't find user account '" + user.getName() + "'");
    } else {
      usersById.remove(user.getId());
      usersByName.remove(user.getName());
      LOG.info("Deleted user account: " + user.getName());
    }
  }

  @Override
  public synchronized void insertSchema(Schema schema) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, Schema> schemas = schemasByName.get(schema.getOwner().getId());
    if (schemas != null) {
      if (schemas.containsKey(schema.getSchemaName())) {
        throw new AlreadyStoredException(schema.getSchemaName() + " already exists");
      }
    } else {
      schemas = Maps.newHashMap();
      schemasByName.put(schema.getOwner().getId(), schemas);
    }

    schema.setId(generateUniqueId());
    schema.setCreateTime(new Date());

    schemas.put(schema.getSchemaName(), schema.clone());

    LOG.info("Added schema " + schema.getSchemaName());
  }

  @Override
  public synchronized Schema findSchema(String schemaName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    if (!schemasByName.containsKey(owner.getId())
        || !schemasByName.get(owner.getId()).containsKey(schemaName)) {
      throw new NotStoredException(schemaName + " isn't registered");
    } else {
      return schemasByName.get(owner.getId()).get(schemaName).clone();
    }
  }

  @Override
  public synchronized boolean changeSchemasToUsed(Schema schema, String topologyId)
      throws MetaStoreException {
    if (!schemasByName.containsKey(schema.getOwner().getId())
        || !schemasByName.get(schema.getOwner().getId()).containsKey(schema.getSchemaName())) {
      String error = "Schema " + schema.getId() + " not stored in meta store.";
      LOG.error(error);
      throw new MetaStoreException(error);
    }

    Schema storedSchema = schemasByName.get(schema.getOwner().getId()).get(schema.getSchemaName());
    if (!schema.getCreateTime().equals(storedSchema.getCreateTime())) {
      return false;
    }

    storedSchema.getTopologies().add(topologyId);
    LOG.info("Change schema to used: Adding topology " + topologyId + " to schema "
        + schema.getId());
    return true;
  }

  @Override
  public synchronized boolean changeSchemasToFree(Schema schema, String topologyId)
      throws MetaStoreException {
    if (!schemasByName.containsKey(schema.getOwner().getId())
        || !schemasByName.get(schema.getOwner().getId()).containsKey(schema.getSchemaName())) {
      String error = "Schema " + schema.getId() + " not stored in meta store.";
      LOG.error(error);
      throw new MetaStoreException(error);
    }

    Schema storedSchema = schemasByName.get(schema.getOwner().getId()).get(schema.getSchemaName());
    if (!schema.getCreateTime().equals(storedSchema.getCreateTime())) {
      return false;
    }

    if (storedSchema.getTopologies().contains(topologyId)) {
      storedSchema.getTopologies().remove(topologyId);
      LOG.info("Change schema to free: Removed topology " + topologyId + " from schema "
          + schema.getId());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized List<Schema> findSchemas(UserEntity owner) throws MetaStoreException {
    List<Schema> results = Lists.newArrayList();
    if (schemasByName.containsKey(owner.getId())) {
      for (Schema schema : schemasByName.get(owner.getId()).values()) {
        results.add(schema.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized boolean deleteSchema(Schema schema) throws MetaStoreException,
      NotStoredException {
    if (!schemasByName.containsKey(schema.getOwner().getId())
        || !schemasByName.get(schema.getOwner().getId()).containsKey(schema.getSchemaName())) {
      throw new NotStoredException("Can't find schema '" + schema.getSchemaName() + "'");
    } else {
      if (schema.getTopologies().isEmpty()) {
        schemasByName.get(schema.getOwner().getId()).remove(schema.getSchemaName());
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public synchronized void insertTopology(GungnirTopology topology) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, GungnirTopology> topologies = topologiesByName.get(topology.getOwner().getId());
    if (topologies != null) {
      if (topologies.containsKey(topology.getName())) {
        throw new AlreadyStoredException(topology.getName() + " already exists");
      }
    } else {
      topologies = Maps.newHashMap();
      topologiesByName.put(topology.getOwner().getId(), topologies);
    }

    topology.setId(generateUniqueId());
    topology.setCreateTime(new Date());

    GungnirTopology topologyCopy = topology.clone();

    topologiesById.put(topology.getId(), topologyCopy);
    topologies.put(topology.getName(), topologyCopy);

    LOG.info("Inserted topology " + topology.getId() + " in meta store.");
  }

  @Override
  public synchronized boolean changeTopologyStatus(GungnirTopology topology)
      throws MetaStoreException {
    LOG.info("Changing status for topology {}", topology.getId());
    if (!topologiesById.containsKey(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    } else {

      TopologyStatus currentStatus = null;
      switch (topology.getStatus()) {
        case STARTING:
          currentStatus = TopologyStatus.STOPPED;
          break;
        case RUNNING:
          currentStatus = TopologyStatus.STARTING;
          break;
        case STOPPING:
          currentStatus = TopologyStatus.RUNNING;
          break;
        case STOPPED:
          currentStatus = TopologyStatus.STOPPING;
          break;
        default:
          return false;
      }

      GungnirTopology storedTopology = topologiesById.get(topology.getId());
      if (storedTopology.getStatus() == currentStatus) {
        storedTopology.setStatus(topology.getStatus());
        return true;
      }
      return false;
    }
  }

  @Override
  public synchronized void changeForcedTopologyStatus(GungnirTopology topology)
      throws MetaStoreException {
    LOG.info("Changing status for topology {}", topology.getId());
    if (!topologiesById.containsKey(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }
    topologiesById.get(topology.getId()).setStatus(topology.getStatus());
  }

  @Override
  public synchronized List<GungnirTopology> findTopologies(UserEntity owner)
      throws MetaStoreException {
    List<GungnirTopology> results = Lists.newArrayList();
    for (GungnirTopology topology : topologiesById.values()) {
      if (topology.getOwner().getName().equals(owner.getName())) {
        results.add(topology.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized List<GungnirTopology> findTopologies(UserEntity owner, TopologyStatus status)
      throws MetaStoreException {
    List<GungnirTopology> results = Lists.newArrayList();
    for (GungnirTopology topology : findTopologies(owner)) {
      if (topology.getStatus() == status) {
        results.add(topology.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized GungnirTopology findTopologyById(String topologyId)
      throws MetaStoreException,
      NotStoredException {
    LOG.info("Retrieving topology " + topologyId);
    GungnirTopology topology = topologiesById.get(topologyId);
    if (topology == null) {
      throw new NotStoredException("Can't find topology '" + topologyId + "'");
    } else {
      return topology.clone();
    }
  }

  @Override
  public synchronized GungnirTopology findTopologyByName(String topologyName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    LOG.info("Retrieving topology " + topologyName);
    if (!topologiesByName.containsKey(owner.getId())
        || !topologiesByName.get(owner.getId()).containsKey(topologyName)) {
      throw new NotStoredException("Can't find topology '" + topologyName + "'");
    } else {
      return topologiesByName.get(owner.getId()).get(topologyName).clone();
    }
  }

  @Override
  public synchronized TopologyStatus getTopologyStatus(String topologyId)
      throws MetaStoreException, NotStoredException {
    return findTopologyById(topologyId).getStatus();
  }

  @Override
  public synchronized void deleteTopology(GungnirTopology topology) throws MetaStoreException {
    if (!topologiesByName.containsKey(topology.getOwner().getId())
        || !topologiesByName.get(topology.getOwner().getId()).containsKey(topology.getName())) {
      throw new MetaStoreException("Can't find topology '" + topology.getName() + "'");
    } else {
      topologiesById.remove(topology.getId());
      topologiesByName.get(topology.getOwner().getId()).remove(topology.getName());
      LOG.info("Removed topology " + topology.getId());
    }
  }

  @Override
  public synchronized String generateTrackingId() throws MetaStoreException {
    String id = generateUniqueId();
    trackingIds.put(id, ++currentTrackingId);
    LOG.info("Generated tracking id " + id + " with value " + currentTrackingId);
    return id;
  }

  @Override
  public synchronized Integer getTrackingNo(String tid) throws MetaStoreException,
      NotStoredException {
    if (!trackingIds.containsKey(tid)) {
      throw new NotStoredException("Tracking id '" + tid + "' not stored in meta store.");
    } else {
      return trackingIds.get(tid);
    }
  }

  private String generateUniqueId() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
