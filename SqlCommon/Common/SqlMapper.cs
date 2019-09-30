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
    /// <summary>
    /// Extended connection
    /// </summary>
    public static class SqlMapper
    {
        /// <summary>
        /// Type mapper
        /// </summary>
        public static ITypeMapper TypeMapper = new TypeMapper();
        /// <summary>
        /// Does name matching ignore underscores
        /// </summary>
        public static bool MatchNamesWithUnderscores { get; set; }
        /// <summary>
        /// Executes a query, returning the data typed as T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"> Timeout</param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static IEnumerable<T> ExecuteQuery<T>(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
                using (var reader = cmd.ExecuteReader())
                {
                    var handler = TypeConvert.GetSerializer<T>(reader);
                    while (reader.Read())
                    {
                        yield return handler(reader);
                    }
                }
            }
        }
        /// <summary>
        /// Execute parameterized SQL
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static int ExecuteNonQuery(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
                return cmd.ExecuteNonQuery();
            }
        }
        /// <summary>
        /// Executes a query, returning the data typed as T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="cancelToken"></param>
        /// <param name="commandTimeout"> Timeout</param>
        /// <param name="commandType"> Type</param>
        /// <returns></returns>
        public async static Task<IEnumerable<T>> ExecuteQueryAsync<T>(this DbConnection connection, string sql, object param = null, IDbTransaction transaction = null, CancellationToken? cancelToken = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                await (cancelToken == null ? connection.OpenAsync() : connection.OpenAsync(cancelToken.Value));
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
                using (var reader = await (cancelToken == null ? cmd.ExecuteReaderAsync() : cmd.ExecuteReaderAsync(cancelToken.Value)))
                {
                    var list = new List<T>();
                    var handler = TypeConvert.GetSerializer<T>(reader);
                    while (await reader.ReadAsync())
                    {
                        list.Add(handler(reader));
                    }
                    return list;
                }
            }
        }
        /// <summary>
        /// Execute a command asynchronously using Task.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="cancelToken"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public async static Task<int> ExecuteNonQueryAsync(this DbConnection connection, string sql, object param = null, IDbTransaction transaction = null, CancellationToken? cancelToken = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                await (cancelToken == null ? connection.OpenAsync() : connection.OpenAsync(cancelToken.Value));
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
                return await (cancelToken == null ? cmd.ExecuteNonQueryAsync() : cmd.ExecuteNonQueryAsync(cancelToken.Value));
            }
        }
        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static T ExecuteScalar<T>(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
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
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="transaction"></param>
        /// <param name="cancelToken"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public async static Task<T> ExecuteScalarAsync<T>(this DbConnection connection, string sql, object param = null, IDbTransaction transaction = null, CancellationToken? cancelToken = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                await (cancelToken == null ? connection.OpenAsync() : connection.OpenAsync(cancelToken.Value));
            using (var cmd = connection.CreateCommand())
            {
                Initialize(cmd, transaction, sql, param, commandTimeout, commandType);
                var result = await (cancelToken == null ? cmd.ExecuteScalarAsync() : cmd.ExecuteScalarAsync(cancelToken.Value));
                if (result is DBNull)
                {
                    return default;
                }
                return (T)Convert.ChangeType(result, typeof(T));
            }
        }
        private static void Initialize(IDbCommand cmd, IDbTransaction transaction, string sql, object param, int? commandTimeout = null, CommandType? commandType = null)
        {
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
            if (param != null)
            {
                var handler = TypeConvert.Deserializer(param);
                var values = handler(param);
                foreach (var item in values)
                {
                    var pattern = $@"in\s+([\@,\:,\?]?{item.Key})";
                    if (Regex.IsMatch(cmd.CommandText, pattern))
                    {
                        var name = Regex.Match(cmd.CommandText, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Multiline).Groups[1].Value;
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
                                var parameter = cmd.CreateParameter();
                                parameter.ParameterName = $"{item.Key}{list.IndexOf(iitem)}";
                                parameter.Value = iitem;
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
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = item.Key;
                        parameter.Value = item.Value;
                        cmd.Parameters.Add(parameter);
                    }
                }
            }
        }

    }

}
