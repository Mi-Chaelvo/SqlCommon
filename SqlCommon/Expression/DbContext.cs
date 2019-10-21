using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlCommon.Linq
{
    public enum DbContextState
    {
        Closed = 0,
        Open = 1,
        Commit = 2,
        Rollback = 3,
    }
    public enum DbContextType
    {
        Mysql,
        SqlServer,
        Oracle,
        Sqlite,
    }
    public struct DbContextLogger
    {
        public string Text { get; set; }
        public object Value { get; set; }
    }
    public interface IDbContext : IDisposable
    {
        DbContextState DbContextState { get; }
        DbContextType DbContextType { get; }
        List<DbContextLogger> Loggers { get; }
        int ExecuteNonQuery(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text);
        Task<int> ExecuteNonQueryAsync(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text);
        T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text);
        Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text);
        IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text);
        (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text);
        Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = CommandType.Text);
        Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);
        IDbConnection Connection { get; }
        IDbTransaction Transaction { get; }
        void Open(bool beginTransaction = false, IsolationLevel? isolationLevel = null);
        Task OpenAsync(bool beginTransaction = false, IsolationLevel? isolationLevel = null);
        void Commit();
        void Rollback();
        void Close();
    }
    public class DbContext : IDbContext
    {
        public DbContext(IDbConnection connection, DbContextType contextType)
        {
            Connection = connection;
            DbContextType = contextType;
        }
        public DbContextState DbContextState { get; private set; } = DbContextState.Closed;
        public DbContextType DbContextType { get; private set; }
        public List<DbContextLogger> Loggers { get; private set; } = new List<DbContextLogger>();
        public IDbConnection Connection { get; private set; }
        public IDbTransaction Transaction { get; private set; }
        public void Close()
        {
            try
            {
                Connection?.Close();
                Transaction?.Dispose();
            }
            catch (Exception)
            {
            }
            finally
            {
                DbContextState = DbContextState.Closed;
            }
        }
        public void Commit()
        {
            DbContextState = DbContextState.Commit;
            Transaction?.Commit();
        }
        public void Dispose()
        {
            Close();
        }
        public (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            return Connection.ExecuteQuery<T1, T2>(sql, param, Transaction, commandTimeout, commandType);
        }
        public Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            return (Connection as DbConnection).ExecuteQueryAsync<T1, T2>(sql, param, Transaction, commandTimeout, commandType);
        }
        public Task<int> ExecuteNonQueryAsync(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            return (Connection as DbConnection).ExecuteNonQueryAsync(sql, param, Transaction, commandTimeout, commandType);
        }
        public int ExecuteNonQuery(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            return Connection.ExecuteNonQuery(sql, param, Transaction, commandTimeout, commandType);
        }
        public IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            return Connection.ExecuteQuery<T>(sql, param, Transaction, commandTimeout, commandType);
        }
        public Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            return (Connection as DbConnection).ExecuteQueryAsync<T>(sql, param, Transaction, commandTimeout, commandType);
        }
        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            return Connection.ExecuteScalar<T>(sql, param, Transaction, commandTimeout, commandType);
        }
        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            return (Connection as DbConnection).ExecuteScalarAsync<T>(sql, param, Transaction, commandTimeout, commandType);
        }
        public void Open(bool beginTransaction = false, IsolationLevel? isolationLevel = IsolationLevel.ReadUncommitted)
        {

            Connection.Open();
            if (beginTransaction)
            {
                Transaction = isolationLevel == null ? Connection.BeginTransaction() : Connection.BeginTransaction(isolationLevel.Value);
            }
            DbContextState = DbContextState.Open;
        }
        public async Task OpenAsync(bool beginTransaction = false, IsolationLevel? isolationLevel = null)
        {
            var dbConnection = Connection as DbConnection;
            await dbConnection.OpenAsync();
            if (beginTransaction)
            {
                Transaction = isolationLevel == null ? dbConnection.BeginTransaction() : Connection.BeginTransaction(isolationLevel.Value);
            }
            DbContextState = DbContextState.Open;
        }
        public void Rollback()
        {
            Transaction?.Rollback();
            DbContextState = DbContextState.Rollback;
        }
    }
    public class DbProxyContext : IDbContext
    {
        private IDbContext _DbContext { get; set; }
        public DbContextState DbContextState => _DbContext.DbContextState;
        public DbContextType DbContextType => _DbContext.DbContextType;
        public List<DbContextLogger> Loggers => _DbContext.Loggers;
        public IDbConnection Connection => _DbContext.Connection;
        public IDbTransaction Transaction => _DbContext.Transaction;
        public DbProxyContext(IDbConnection connection, DbContextType contextType)
        {
            _DbContext = new DbContext(connection, contextType);
        }
        public DbProxyContext(IDbContext dbContext)
        {
            _DbContext = dbContext;
        }
        public int ExecuteNonQuery(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteNonQuery(sql, param, commandTimeout, commandType);
        }
        public Task<int> ExecuteNonQueryAsync(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteNonQueryAsync(sql, param, commandTimeout, commandType);
        }
        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteScalar<T>(sql, param, commandTimeout, commandType);
        }
        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteScalarAsync<T>(sql, param, commandTimeout, commandType);
        }
        public IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteQuery<T>(sql, param, buffered, commandTimeout, commandType);
        }
        public (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, bool buffered = false, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteQuery<T1, T2>(sql, param, buffered, commandTimeout, commandType);
        }
        public Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = CommandType.Text)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteQueryAsync<T>(sql, param, commandTimeout, commandType);
        }
        public Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            WriteLogger(sql, param);
            return _DbContext.ExecuteQueryAsync<T1, T2>(sql, param, commandTimeout, commandType);
        }
        public void Open(bool beginTransaction = false, IsolationLevel? isolationLevel = null)
        {
            _DbContext.Open(beginTransaction, isolationLevel);
        }
        public Task OpenAsync(bool beginTransaction = false, IsolationLevel? isolationLevel = null)
        {
            return _DbContext.OpenAsync(beginTransaction, isolationLevel);
        }
        public void Commit()
        {
            _DbContext.Commit();
        }
        public void Rollback()
        {
            _DbContext.Rollback();
        }
        public void Close()
        {
            _DbContext.Close();
        }
        public void Dispose()
        {
            _DbContext.Dispose();
        }
        private void WriteLogger(string sql, object param)
        {
            Loggers.Add(new DbContextLogger()
            {
                Text = sql,
                Value = param
            });
        }
    }
    public static class DbContextExtension
    {
        public static IQueryable<T> From<T>(this IDbContext dbContext, string viewName) where T : class
        {
            return new SqlQuery<T>(dbContext, viewName);
        }
        public static IQueryable<T> From<T>(this IDbContext dbContext, bool buffered = false, int? timeout = null) where T : class
        {
            return new SqlQuery<T>(dbContext, null, buffered, timeout);
        }
        public static IQueryable<T1, T2> From<T1, T2>(this IDbContext dbContext, string viewName)
            where T1 : class
            where T2 : class
        {
            return new SqlQuery<T1, T2>(dbContext, viewName);
        }
        public static IQueryable<T1, T2> From<T1, T2>(this IDbContext dbContext, int? timeout = null)
         where T1 : class
         where T2 : class
        {
            return new SqlQuery<T1, T2>(dbContext, null, timeout);
        }
        public static IQueryable<T1, T2, T3> From<T1, T2, T3>(this IDbContext dbContext, string viewName)
            where T1 : class
            where T2 : class
            where T3 : class
        {
            return new SqlQuery<T1, T2, T3>(dbContext, viewName);
        }
        public static IQueryable<T1, T2, T3> From<T1, T2, T3>(this IDbContext dbContext, int? timeout = null)
           where T1 : class
           where T2 : class
           where T3 : class
        {
            return new SqlQuery<T1, T2, T3>(dbContext, null, timeout);
        }
        public static IQueryable<T1, T2, T3, T4> From<T1, T2, T3, T4>(this IDbContext dbContext, string viewName)
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
        {
            return new SqlQuery<T1, T2, T3, T4>(dbContext, viewName);
        }
        public static IQueryable<T1, T2, T3, T4> From<T1, T2, T3, T4>(this IDbContext dbContext, int? timeout = null)
           where T1 : class
           where T2 : class
           where T3 : class
           where T4 : class
        {
            return new SqlQuery<T1, T2, T3, T4>(dbContext, null, timeout);
        }
    }
}
