using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Collections;

namespace SqlContext
{
    public static class SqlMapper
    {
        public static IEnumerable<T> ExecuteQuery<T>(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                if (commandTimeout.HasValue)
                {
                    cmd.CommandTimeout = commandTimeout.Value;
                }
                if (commandType.HasValue)
                {
                    cmd.CommandType = commandType.Value;
                }

                Initialize(cmd, param);

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
        public static int ExecuteNonQuery(this IDbConnection connection, string sql, object param = null)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                Initialize(cmd, param);
                return cmd.ExecuteNonQuery();
            }
        }
        private static void Initialize(IDbCommand cmd, object param)
        {
            if (param != null)
            {
                var handler = TypeConvert.Deserializer(param);
                var values = handler(param);
                foreach (var item in values)
                {
                    object value = item.Value;
                    if (item.Value is IEnumerable<object> || item.Value is IEnumerable<ValueType>)
                    {
                        var prefix = string.Empty;
                        var name = string.Empty;
                        var pattern = $@"[\@,\:,\?]?{item.Key}";
                        name = Regex.Match(cmd.CommandText, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Multiline).Value;
                        var list = (item.Value as IEnumerable).Cast<object>().Where(a => a != null && a != DBNull.Value).ToList();
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
                        parameter.Value = value;
                        cmd.Parameters.Add(parameter);
                    }
                }
            }
        }

    }

}
