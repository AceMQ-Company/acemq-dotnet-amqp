// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>Opens a connection to the database these stores use.</summary>
/// <remarks>
/// A factory rather than a connection, because a connection is not safe to share
/// across threads and these stores are called from consumers that run concurrently.
/// </remarks>
public delegate DbConnection ConnectionSupplier();

/// <summary>
/// An outbox kept in a database.
/// </summary>
/// <remarks>
/// <para>
/// This is the one that makes the pattern work. The point of an outbox is that the
/// message and the business change commit together, and that requires them to be in
/// the same database — which the in-memory store cannot be.
/// </para>
/// <para>
/// <see cref="AddAsync(OutboxRecord)"/> opens its own connection and is therefore
/// <em>not</em> in your transaction. To get the guarantee, use
/// <see cref="AddAsync(OutboxRecord, DbTransaction)"/> and pass the transaction the
/// business change is being written in.
/// </para>
/// <para>
/// Plain ADO.NET, so it works against SQL Server, PostgreSQL, SQLite, MySQL or
/// anything else with a provider. The parameter prefix differs between them and is
/// configurable; everything else is standard SQL.
/// </para>
/// </remarks>
public sealed class DbOutboxStore : IOutboxStore
{
    private readonly ConnectionSupplier _connections;
    private readonly string _table;
    private readonly string _prefix;

    public DbOutboxStore(ConnectionSupplier connections)
        : this(connections, "acemq_outbox", "@") { }

    /// <remarks>
    /// <paramref name="parameterPrefix"/> is <c>@</c> for SQL Server and SQLite,
    /// <c>:</c> for Oracle, and either for PostgreSQL depending on the provider.
    /// </remarks>
    public DbOutboxStore(ConnectionSupplier connections, string table, string parameterPrefix)
    {
        _connections = connections ?? throw new ArgumentNullException(nameof(connections));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _prefix = parameterPrefix ?? throw new ArgumentNullException(nameof(parameterPrefix));
    }

    /// <summary>The table this store expects, in standard SQL.</summary>
    /// <remarks>
    /// Offered rather than created. A library that silently runs DDL against an
    /// application's database is a library that has done something irreversible
    /// without being asked; this hands you the statement to put in a migration.
    /// </remarks>
    public string CreateTableSql() =>
        $@"CREATE TABLE {_table} (
  id             VARCHAR(64)  NOT NULL PRIMARY KEY,
  exchange       VARCHAR(255) NOT NULL,
  routing_key    VARCHAR(255) NOT NULL,
  type           VARCHAR(255) NOT NULL,
  payload        TEXT         NOT NULL,
  correlation_id VARCHAR(64)      NULL,
  causation_id   VARCHAR(64)      NULL,
  created_at     TIMESTAMP    NOT NULL,
  attempts       INTEGER      NOT NULL,
  last_error     TEXT             NULL,
  published      INTEGER      NOT NULL,
  leased_until   TIMESTAMP        NULL
);
CREATE INDEX {_table}_pending ON {_table} (published, leased_until, created_at);";

    public Task AddAsync(OutboxRecord record) => AddAsync(record, null);

    /// <summary>
    /// Writes the record inside a transaction you already have open.
    /// </summary>
    /// <remarks>
    /// This is the whole point of a database-backed outbox. Passing the transaction
    /// the business change is in means the two commit or roll back together, and
    /// there is no window in which one exists without the other.
    /// </remarks>
    public async Task AddAsync(OutboxRecord record, DbTransaction? transaction)
    {
        if (record == null) throw new ArgumentNullException(nameof(record));

        var sql = $@"INSERT INTO {_table}
  (id, exchange, routing_key, type, payload, correlation_id, causation_id,
   created_at, attempts, last_error, published, leased_until)
VALUES ({P("id")}, {P("exchange")}, {P("routingKey")}, {P("type")}, {P("payload")},
        {P("correlationId")}, {P("causationId")}, {P("createdAt")}, {P("attempts")},
        NULL, 0, NULL)";

        if (transaction?.Connection != null)
        {
            using var owned = Command(transaction.Connection, sql, transaction);
            Bind(owned, record);
            await ExecuteAsync(owned).ConfigureAwait(false);
            return;
        }

        using var connection = Open();
        using var command = Command(connection, sql, null);
        Bind(command, record);
        await ExecuteAsync(command).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(int batchSize, TimeSpan lease)
    {
        var now = DateTimeOffset.UtcNow;
        var until = now + lease;
        var claimed = new List<OutboxRecord>();

        using var connection = Open();

        // Selected and then claimed by id, rather than with UPDATE ... RETURNING,
        // which not every provider supports. The claim is conditional on the lease
        // still being free, so two relays racing for the same row cannot both win:
        // the second one updates zero rows and drops it.
        var select = $@"SELECT id, exchange, routing_key, type, payload, correlation_id,
       causation_id, created_at, attempts, last_error
FROM {_table}
WHERE published = 0 AND (leased_until IS NULL OR leased_until <= {P("now")})
ORDER BY created_at";

        var candidates = new List<OutboxRecord>();
        using (var command = Command(connection, select, null))
        {
            Add(command, "now", now.UtcDateTime);
            using var reader = command.ExecuteReader();
            while (reader.Read() && candidates.Count < batchSize * 2)
            {
                candidates.Add(Read(reader));
            }
        }

        foreach (var record in candidates)
        {
            if (claimed.Count >= batchSize) break;
            var claim = $@"UPDATE {_table} SET leased_until = {P("until")}
WHERE id = {P("id")} AND published = 0
  AND (leased_until IS NULL OR leased_until <= {P("now")})";
            using var command = Command(connection, claim, null);
            Add(command, "until", until.UtcDateTime);
            Add(command, "id", record.Id);
            Add(command, "now", now.UtcDateTime);
            if (command.ExecuteNonQuery() == 1) claimed.Add(record);
        }

        return claimed;
    }

    public async Task MarkPublishedAsync(string id)
    {
        using var connection = Open();
        using var command = Command(
            connection,
            $"UPDATE {_table} SET published = 1, leased_until = NULL WHERE id = {P("id")}",
            null);
        Add(command, "id", id);
        await ExecuteAsync(command).ConfigureAwait(false);
    }

    public async Task MarkFailedAsync(string id, string reason)
    {
        using var connection = Open();
        // The lease is released so the next pass picks it up, and the attempt count
        // carries so a record that always fails is visible rather than merely slow.
        using var command = Command(
            connection,
            $@"UPDATE {_table}
SET attempts = attempts + 1, last_error = {P("reason")}, leased_until = NULL
WHERE id = {P("id")}",
            null);
        Add(command, "reason", reason ?? string.Empty);
        Add(command, "id", id);
        await ExecuteAsync(command).ConfigureAwait(false);
    }

    public Task<long> PendingCountAsync()
    {
        using var connection = Open();
        using var command = Command(connection, $"SELECT COUNT(*) FROM {_table} WHERE published = 0", null);
        return Task.FromResult(Convert.ToInt64(
            command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture));
    }

    private string P(string name) => _prefix + name;

    private DbConnection Open()
    {
        var connection = _connections();
        if (connection.State != ConnectionState.Open) connection.Open();
        return connection;
    }

    private DbCommand Command(DbConnection connection, string sql, DbTransaction? transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction != null) command.Transaction = transaction;
        return command;
    }

    private void Add(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = _prefix + name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    private void Bind(DbCommand command, OutboxRecord record)
    {
        Add(command, "id", record.Id);
        Add(command, "exchange", record.Exchange);
        Add(command, "routingKey", record.RoutingKey);
        Add(command, "type", record.Type);
        Add(command, "payload", record.Payload);
        Add(command, "correlationId", record.CorrelationId);
        Add(command, "causationId", record.CausationId);
        Add(command, "createdAt", record.CreatedAt.UtcDateTime);
        Add(command, "attempts", record.Attempts);
    }

    private static OutboxRecord Read(DbDataReader reader) =>
        new OutboxRecord(
            reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)),
            reader.GetInt32(8),
            reader.IsDBNull(9) ? null : reader.GetString(9));

    private static Task ExecuteAsync(DbCommand command)
    {
        command.ExecuteNonQuery();
        return Task.CompletedTask;
    }

    public override string ToString() => $"DbOutboxStore[{_table}]";
}

/// <summary>
/// An idempotency store kept in a database.
/// </summary>
/// <remarks>
/// Unlike the in-memory one, this deduplicates across instances and across
/// restarts, which is what is needed when more than one consumer reads the same
/// queue. The claim is an insert, so the database's primary key does the mutual
/// exclusion: two consumers racing for the same message, one insert succeeds.
/// </remarks>
public sealed class DbIdempotencyStore : IIdempotencyStore
{
    private readonly ConnectionSupplier _connections;
    private readonly string _table;
    private readonly string _prefix;
    private readonly TimeSpan _retention;

    public DbIdempotencyStore(ConnectionSupplier connections, TimeSpan retention)
        : this(connections, retention, "acemq_idempotency", "@") { }

    public DbIdempotencyStore(
        ConnectionSupplier connections, TimeSpan retention, string table, string parameterPrefix)
    {
        _connections = connections ?? throw new ArgumentNullException(nameof(connections));
        _retention = retention;
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _prefix = parameterPrefix ?? throw new ArgumentNullException(nameof(parameterPrefix));
    }

    /// <summary>The table this store expects.</summary>
    public string CreateTableSql() =>
        $@"CREATE TABLE {_table} (
  message_id VARCHAR(255) NOT NULL PRIMARY KEY,
  claimed_at TIMESTAMP    NOT NULL,
  confirmed  INTEGER      NOT NULL
);
CREATE INDEX {_table}_claimed ON {_table} (claimed_at);";

    public Task<bool> ClaimAsync(string messageId)
    {
        if (messageId == null) throw new ArgumentNullException(nameof(messageId));
        using var connection = Open();
        Evict(connection);

        try
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"INSERT INTO {_table} (message_id, claimed_at, confirmed) " +
                $"VALUES ({_prefix}id, {_prefix}at, 0)";
            Add(command, "id", messageId);
            Add(command, "at", DateTime.UtcNow);
            command.ExecuteNonQuery();
            return Task.FromResult(true);
        }
        catch (DbException)
        {
            // The primary key rejected it, so somebody else has the claim. This is
            // the mutual exclusion, and it is the database's rather than ours --
            // a select-then-insert would have a window between the two.
            return Task.FromResult(false);
        }
    }

    public Task ConfirmAsync(string messageId)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            $"UPDATE {_table} SET confirmed = 1 WHERE message_id = {_prefix}id";
        Add(command, "id", messageId);
        command.ExecuteNonQuery();
        return Task.CompletedTask;
    }

    public Task ReleaseAsync(string messageId)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {_table} WHERE message_id = {_prefix}id";
        Add(command, "id", messageId);
        command.ExecuteNonQuery();
        return Task.CompletedTask;
    }

    public Task<bool> IsConfirmedAsync(string messageId)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT confirmed FROM {_table} WHERE message_id = {_prefix}id";
        Add(command, "id", messageId);
        var value = command.ExecuteScalar();
        return Task.FromResult(
            value != null && value != DBNull.Value
            && Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture) == 1);
    }

    private void Evict(DbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {_table} WHERE claimed_at < {_prefix}before";
        Add(command, "before", DateTime.UtcNow - _retention);
        command.ExecuteNonQuery();
    }

    private DbConnection Open()
    {
        var connection = _connections();
        if (connection.State != ConnectionState.Open) connection.Open();
        return connection;
    }

    private void Add(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = _prefix + name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    public override string ToString() => $"DbIdempotencyStore[{_table}]";
}
