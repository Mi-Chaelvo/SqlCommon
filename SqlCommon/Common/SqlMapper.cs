using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Collections;
using System.Data.Common;
using System.Threading;

namespace SqlCommon
{
    public interface ISqlMapper
    {
        IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        int ExecuteNonQuery(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        Task<int> ExecuteNonQueryAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        T ExecuteScalar<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
        Task<T> ExecuteScalarAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null);
    }
    /// <summary>
    /// Execute script
    /// </summary>
    public class SqlMapper : ISqlMapper
    {
        private readonly IDbConnection Connection = null;
        private readonly ITypeMapper TypeMapper = null;
        public SqlMapper(IDbConnection connection, ITypeMapper typeMapper)
        {
            Connection = connection;
            TypeMapper = typeMapper;
        }
        /// <summary>
        /// Executes a query, returning the data typed as T.
        /// </summary>
        public IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = Connection.CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                using (var reader = cmd.ExecuteReader())
                {
                    var handler = TypeConvert.GetSerializer<T>(TypeMapper, reader);
                    while (reader.Read())
                    {
                        yield return handler(reader);
                    }
                }
            }
        }
        /// <summary>
        /// Executes Multi query, returning the data typed as valueTuple.
        /// </summary>
        public (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = Connection.CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                var item1 = new List<T1>();
                var item2 = new List<T2>();
                using (var reader = cmd.ExecuteReader())
                {
                    var count = 0;
                    do
                    {
                        if (count == 0)
                        {
                            var handler = TypeConvert.GetSerializer<T1>(TypeMapper, reader);
                            while (reader.Read())
                                item1.Add(handler(reader));
                        }
                        if (count == 1)
                        {
                            var handler = TypeConvert.GetSerializer<T2>(TypeMapper, reader);
                            while (reader.Read())
                                item2.Add(handler(reader));
                        }
                        count++;
                    } while (reader.NextResult());
                    return (item1, item2);
                }
            }

        }
        /// <summary>
        /// Executes a query, returning the data typed as T
        /// </summary>
        public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = (Connection as DbConnection).CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var list = new List<T>();
                    var handler = TypeConvert.GetSerializer<T>(TypeMapper, reader);
                    while (await reader.ReadAsync())
                    {
                        list.Add(handler(reader));
                    }
                    return list;
                }
            }
        }
        /// <summary>
        ///Executes Multi query, returning the data typed as valueTuple.
        /// </summary>
        public async Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = (Connection as DbConnection).CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                var item1 = new List<T1>();
                var item2 = new List<T2>();
                var count = 0;
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    do
                    {
                        if (count == 0)
                        {
                            var handler = TypeConvert.GetSerializer<T1>(TypeMapper, reader);
                            while (await reader.ReadAsync())
                                item1.Add(handler(reader));
                        }
                        if (count == 1)
                        {
                            var handler = TypeConvert.GetSerializer<T2>(TypeMapper, reader);
                            while (await reader.ReadAsync())
                                item2.Add(handler(reader));
                        }
                        count++;
                    } while (reader.NextResult());
                    return (item1, item2);
                }
            }
        }
        /// <summary>
        /// Execute parameterized SQL
        /// </summary>
        public int ExecuteNonQuery(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = Connection.CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                return cmd.ExecuteNonQuery();
            }
        }
        /// <summary>
        /// Execute a command asynchronously using Task.
        /// </summary>
        public async Task<int> ExecuteNonQueryAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = (Connection as DbConnection).CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                return await cmd.ExecuteNonQueryAsync();
            }
        }
        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        public T ExecuteScalar<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = Connection.CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                var result = cmd.ExecuteScalar();
                if (result is DBNull)
                {
                    return default;
                }
                return (T)Convert.ChangeType(result, typeof(T));
            }
        }
        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        public async Task<T> ExecuteScalarAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var cmd = (Connection as DbConnection).CreateCommand())
            {
                Initialize(cmd, sql, param, transaction, commandTimeout, commandType);
                var result = await cmd.ExecuteScalarAsync();
                if (result is DBNull)
                {
                    return default;
                }
                return (T)Convert.ChangeType(result, typeof(T));
            }
        }
        /// <summary>
        /// handler command
        /// </summary>      
        private void Initialize(IDbCommand cmd, string sql, object param, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var dbParameters = new List<IDbDataParameter>();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            if (commandTimeout.HasValue)
            {
                cmd.CommandTimeout = commandTimeout.Value;
            }
            if (commandType.HasValue)
            {
                cmd.CommandType = commandType.Value;
            }
            if (param is IDbDataParameter)
            {
                dbParameters.Add(param as IDbDataParameter);
            }
            else if (param is IEnumerable<IDbDataParameter> parameters)
            {
                dbParameters.AddRange(parameters);
            }
            else if (param is Dictionary<string, object> keyValues)
            {
                foreach (var item in keyValues)
                {
                    var parameter = CreateParameter(cmd, item.Key, item.Value);
                    dbParameters.Add(parameter);
                }
            }
            else if (param != null)
            {
                var handler = TypeConvert.GetDeserializer(param.GetType());
                var values = handler(param);
                foreach (var item in values)
                {
                    var parameter = CreateParameter(cmd, item.Key, item.Value);
                    dbParameters.Add(parameter);
                }
            }
            if (dbParameters.Count > 0)
            {
                foreach (IDataParameter item in dbParameters)
                {
                    item.Value = item.Value ?? DBNull.Value;
                    var pattern = $@"in\s+([\@,\:,\?]?{item.ParameterName})";
                    var options = RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Multiline;
                    if (cmd.CommandText.IndexOf("in", StringComparison.OrdinalIgnoreCase) != -1 && Regex.IsMatch(cmd.CommandText, pattern, options))
                    {
                        var name = Regex.Match(cmd.CommandText, pattern, options).Groups[1].Value;
                        var list = new List<object>();
                        if (item.Value is IEnumerable<object> || item.Value is Array)
                        {
                            list = (item.Value as IEnumerable).Cast<object>().Where(a => a != null && a != DBNull.Value).ToList();
                        }
                        else
                        {
                            list.Add(item.Value);
                        }
                        if (list.Count() > 0)
                        {
                            cmd.CommandText = Regex.Replace(cmd.CommandText, name, $"({string.Join(",", list.Select(s => $"{name}{list.IndexOf(s)}"))})");
                            foreach (var iitem in list)
                            {
                                var key = $"{item.ParameterName}{list.IndexOf(iitem)}";
                                var parameter = CreateParameter(cmd, key, iitem);
                                cmd.Parameters.Add(parameter);
                            }
                        }
                        else
                        {
                            cmd.CommandText = Regex.Replace(cmd.CommandText, name, $"(SELECT 1 WHERE 1 = 0)");
                        }
                    }
                    else
                    {
                        cmd.Parameters.Add(item);
                    }
                }
            }
        }
        private IDbDataParameter CreateParameter(IDbCommand command, string name, object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }
    }
}
