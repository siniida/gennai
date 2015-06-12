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

package org.gennai.gungnir.ql.session;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class DistributedSessionStore implements SessionStore {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSessionStore.class);

  private static final int GENERATE_SESSION_RETRY_TIMES = 10;

  private int sessionTimeoutSecs;
  private CuratorFramework curator;

  public DistributedSessionStore(CuratorFramework curator) {
    this.curator = curator;
    sessionTimeoutSecs = GungnirManager.getManager().getConfig().getInteger(SESSION_TIMEOUT_SECS);
  }

  public void open() throws SessionStoreException {
    try {
      if (curator.checkExists().forPath(SESSION_NODE_PATH) == null) {
        try {
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(SESSION_NODE_PATH);
        } catch (KeeperException.NodeExistsException ignore) {
          ignore = null;
        }
      }
    } catch (Exception e) {
      throw new SessionStoreException(e);
    }
  }

  private void expiredSession(String accountId) throws SessionStoreException {
    try {
      List<String> sessionIds =
          curator.getChildren().forPath(SESSION_INDEX_NODE_PATH + "/" + accountId);
      for (String sessionId : sessionIds) {
        try {
          SessionEntity session = (SessionEntity) Utils.deserialize(curator.getData().forPath(
              SESSIONS_NODE_PATH + "/" + sessionId));
          if (session.isExpired()) {
            deleteSession(sessionId);
            LOG.info("Expired session {}", sessionId);
          } else {
            curator.setData().forPath(SESSIONS_NODE_PATH + "/" + sessionId,
                Utils.serialize(session));

            LOG.info("Update session timeout. session: {}, timeout: {}", sessionId,
                session.getExpire());
          }
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        }
      }
    } catch (KeeperException.NoNodeException ignore) {
      ignore = null;
    } catch (Exception e) {
      throw new SessionStoreException(e);
    }
  }

  @Override
  public String createSession(UserEntity owner) throws SessionStoreException {
    expiredSession(owner.getId());

    try {
      SessionEntity session = null;
      for (int i = 0; i < GENERATE_SESSION_RETRY_TIMES; i++) {
        try {
          session = new SessionEntity(UUID.randomUUID().toString(), owner.getId(),
              sessionTimeoutSecs);
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(SESSIONS_NODE_PATH + "/" + session.getSessionId(), Utils.serialize(session));

          LOG.info("Successful to create session {}", session.getSessionId());
          break;
        } catch (KeeperException.NodeExistsException ignore) {
          session = null;
        }
      }

      if (session == null) {
        LOG.error("Failed to generate session ID");
        throw new SessionStoreException("Failed to generate session ID");
      }

      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .forPath(SESSION_INDEX_NODE_PATH + "/" + owner.getId() + "/" + session.getSessionId());

      LOG.info("Successful to update index {}", SESSION_INDEX_NODE_PATH + "/" + owner.getId()
          + "/" + session.getSessionId());

      return session.getSessionId();
    } catch (SessionStoreException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to create session", e);
      throw new SessionStoreException(e);
    }
  }

  private void expiredSession(String sessionId, SessionEntity session) throws Exception {
    if (session.isExpired()) {
      deleteSession(sessionId);
      LOG.info("Expired session {}", sessionId);
      throw new InvalidSessionException("This session has been closed");
    } else {
      curator.setData().forPath(SESSIONS_NODE_PATH + "/" + sessionId, Utils.serialize(session));

      LOG.info("Update session timeout. session: {}, timeout: {}", sessionId, session.getExpire());
    }
  }

  @Override
  public String createStatement(String sessionId) throws SessionStoreException,
      InvalidSessionException {
    try {
      SessionEntity session = (SessionEntity) Utils.deserialize(curator.getData().forPath(
          SESSIONS_NODE_PATH + "/" + sessionId));

      expiredSession(sessionId, session);

      UserEntity owner =
          GungnirManager.getManager().getMetaStore().findUserAccountById(session.getAccountId());

      StatementEntity statement = null;
      for (int i = 0; i < GENERATE_SESSION_RETRY_TIMES; i++) {
        try {
          statement = new StatementEntity(UUID.randomUUID().toString(), sessionId, owner);
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(STATEMENTS_NODE_PATH + "/" + statement.getStatementId(),
                  Utils.serialize(statement));

          LOG.info("Successful to create statement {}", statement.getStatementId());
          break;
        } catch (KeeperException.NodeExistsException ignore) {
          statement = null;
        }
      }

      if (statement == null) {
        LOG.error("Failed to generate statement ID");
        throw new SessionStoreException("Failed to generate statement ID");
      }

      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .forPath(SESSION_INDEX_NODE_PATH + "/" + owner.getId() + "/" + sessionId + "/"
              + statement.getStatementId());

      LOG.info("Successful to update index {}", SESSION_INDEX_NODE_PATH + "/" + owner.getId()
          + "/" + sessionId + "/" + statement.getStatementId());

      return statement.getStatementId();
    } catch (KeeperException.NoNodeException e) {
      throw new InvalidSessionException("This session has been closed");
    } catch (InvalidSessionException e) {
      throw e;
    } catch (SessionStoreException e) {
      throw e;
    } catch (MetaStoreException e) {
      LOG.error("Failed to create statement", e);
      throw new SessionStoreException(e);
    } catch (NotStoredException e) {
      throw new InvalidSessionException("This session has been closed");
    } catch (Exception e) {
      LOG.error("Failed to create statement", e);
      throw new SessionStoreException(e);
    }
  }

  @Override
  public StatementEntity getStatement(String statementId) throws SessionStoreException,
      InvalidSessionException {
    StatementEntity statement = null;
    try {
      statement = (StatementEntity) Utils.deserialize(curator.getData().forPath(
          STATEMENTS_NODE_PATH + "/" + statementId));

      SessionEntity session = (SessionEntity) Utils.deserialize(curator.getData().forPath(
          SESSIONS_NODE_PATH + "/" + statement.getSessionId()));

      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      UserEntity owner = metaStore.findUserAccountById(session.getAccountId());
      statement.setOwner(owner);

      if (statement.getTopology() != null) {
        if (statement.getTopology().getId() != null) {
          try {
            TopologyStatus status = metaStore.getTopologyStatus(statement.getTopology().getId());
            statement.getTopology().setStatus(status);
          } catch (NotStoredException e) {
            statement.clear();
          }
        } else {
          statement.getTopology().setOwner(owner);
        }

        if (statement.getTopology() != null) {
          List<Schema> schemas = statement.getTopology().getUsedSchemas();
          for (Schema schema : schemas) {
            schema.setOwner(owner);
          }
        }
      }

      expiredSession(statement.getSessionId(), session);
    } catch (KeeperException.NoNodeException e) {
      throw new InvalidSessionException("This session has been closed");
    } catch (InvalidSessionException e) {
      throw e;
    } catch (MetaStoreException e) {
      LOG.error("Failed to get statement", e);
      throw new SessionStoreException(e);
    } catch (NotStoredException e) {
      throw new InvalidSessionException("This session has been closed");
    } catch (Exception e) {
      LOG.error("Failed to get statement", e);
      throw new SessionStoreException(e);
    }

    return statement;
  }

  @Override
  public void setStatement(String statementId, StatementEntity statement)
      throws SessionStoreException, InvalidSessionException {
    try {
      SessionEntity session = (SessionEntity) Utils.deserialize(curator.getData().forPath(
          SESSIONS_NODE_PATH + "/" + statement.getSessionId()));

      expiredSession(statement.getSessionId(), session);

      statement.setOwner(null);
      byte[] bytes = Utils.serialize(statement);
      curator.setData().forPath(STATEMENTS_NODE_PATH + "/" + statementId, bytes);

      LOG.info("Successful to set statement {}", statementId);
    } catch (KeeperException.NoNodeException e) {
      throw new InvalidSessionException("This session has been closed");
    } catch (InvalidSessionException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to set statement", e);
      throw new SessionStoreException(e);
    }
  }

  @Override
  public void deleteStatement(String statementId) throws SessionStoreException {
    try {
      StatementEntity statement = null;
      try {
        statement = (StatementEntity) Utils.deserialize(curator.getData().forPath(
            STATEMENTS_NODE_PATH + "/" + statementId));
        curator.delete().forPath(STATEMENTS_NODE_PATH + "/" + statementId);

        LOG.info("Successful to delete statement {}", statementId);
      } catch (KeeperException.NoNodeException ignore) {
        ignore = null;
      }

      if (statement != null) {
        try {
          curator.delete().forPath(SESSION_INDEX_NODE_PATH + "/" + statement.getOwner().getId()
              + "/" + statement.getSessionId() + "/" + statementId);

          LOG.info("Successful to update index {}", SESSION_INDEX_NODE_PATH + "/"
              + statement.getOwner().getId() + "/" + statement.getSessionId() + "/" + statementId);
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to delete statement", e);
      throw new SessionStoreException(e);
    }
  }

  @Override
  public void deleteSession(String sessionId) throws SessionStoreException {
    try {
      SessionEntity session = null;
      try {
        session = (SessionEntity) Utils.deserialize(curator.getData().forPath(
            SESSIONS_NODE_PATH + "/" + sessionId));
        curator.delete().forPath(SESSIONS_NODE_PATH + "/" + sessionId);

        LOG.info("Successful to delete session {}", sessionId);
      } catch (KeeperException.NoNodeException ignore) {
        ignore = null;
      }

      if (session != null) {
        try {
          List<String> statementIds = curator.getChildren().forPath(SESSION_INDEX_NODE_PATH + "/"
              + session.getAccountId() + "/" + sessionId);
          for (String statementId : statementIds) {
            try {
              curator.delete().forPath(STATEMENTS_NODE_PATH + "/" + statementId);

              LOG.info("Successful to delete statement {}", statementId);
            } catch (KeeperException.NoNodeException ignore) {
              ignore = null;
            }
          }

          curator.delete().deletingChildrenIfNeeded().forPath(SESSION_INDEX_NODE_PATH + "/"
              + session.getAccountId() + "/" + sessionId);

          LOG.info("Successful to update index {}", SESSION_INDEX_NODE_PATH + "/"
              + session.getAccountId() + "/" + sessionId);
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        }

        try {
          Stat stat = curator.checkExists().forPath(SESSION_INDEX_NODE_PATH + "/"
              + session.getAccountId());
          if (stat.getNumChildren() == 0) {
            curator.delete().forPath(SESSION_INDEX_NODE_PATH + "/" + session.getAccountId());
          }
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        } catch (KeeperException.NotEmptyException ignore) {
          ignore = null;
        }

      }
    } catch (Exception e) {
      LOG.error("Failed to delete session", e);
      throw new SessionStoreException(e);
    }
  }

  @Override
  public void deleteAllSessions(String accountId) throws SessionStoreException {
    try {
      List<String> sessionIds =
          curator.getChildren().forPath(SESSION_INDEX_NODE_PATH + "/" + accountId);
      for (String sessionId : sessionIds) {
        try {
          List<String> statementIds = curator.getChildren().forPath(SESSION_INDEX_NODE_PATH + "/"
              + accountId + "/" + sessionId);
          for (String statementId : statementIds) {
            try {
              curator.delete().forPath(STATEMENTS_NODE_PATH + "/" + statementId);

              LOG.info("Successful to delete statement {}", statementId);
            } catch (KeeperException.NoNodeException ignore) {
              ignore = null;
            }
          }

          curator.delete().forPath(SESSIONS_NODE_PATH + "/" + sessionId);

          LOG.info("Successful to delete session {}", sessionId);
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        }
      }

      curator.delete().deletingChildrenIfNeeded().forPath(SESSION_INDEX_NODE_PATH + "/"
          + accountId);

      LOG.info("Successful to update index {}", SESSION_INDEX_NODE_PATH + "/" + accountId);
    } catch (KeeperException.NoNodeException ignore) {
      ignore = null;
    } catch (Exception e) {
      throw new SessionStoreException(e);
    }
  }

  @Override
  public void close() throws SessionStoreException {
  }
}
