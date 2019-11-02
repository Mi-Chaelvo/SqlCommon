﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Dapper;
using System.Threading.Tasks;
using System.Reflection;
using System.Linq;

namespace Dapper.Extensions
{
    //通过Dapper定义实现SqlMapper
    public class DapperSqlMapper : SqlCommon.ISqlMapper
    {
        static DapperSqlMapper()
        {
            //替换dapper的默认映射使之支持匿名类型
            SqlMapper.TypeMapProvider = s => new LinqTypeMap(s);
        }
        public IDbConnection Connection { get; private set; }
        public DapperSqlMapper(IDbConnection connection)
        {
            Connection = connection;
        }
        public int ExecuteNonQuery(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Connection.Execute(sql,param,transaction,commandTimeout, commandType);
        }

        public Task<int> ExecuteNonQueryAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Connection.ExecuteAsync(sql, param, transaction, commandTimeout, commandType);
        }

        public IEnumerable<T> ExecuteQuery<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Connection.Query<T>(sql, param, transaction, false,commandTimeout, commandType).ToList();
        }

        public (IEnumerable<T1>, IEnumerable<T2>) ExecuteQuery<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var multi = Connection.QueryMultiple(sql, param, transaction, commandTimeout, commandType))
            {
                return (multi.Read<T1>().ToList(), multi.Read<T2>().ToList());
            }
        }

        public Task<IEnumerable<T>> ExecuteQueryAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Connection.QueryAsync<T>(sql,param,transaction,commandTimeout,commandType);
        }

        public async Task<(IEnumerable<T1>, IEnumerable<T2>)> ExecuteQueryAsync<T1, T2>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var multi = await Connection.QueryMultipleAsync(sql, param, transaction, commandTimeout, commandType))
            {
                return (await multi.ReadAsync<T1>(), await multi.ReadAsync<T2>());
            }
        }

        public T ExecuteScalar<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
           return Connection.ExecuteScalar<T>(sql,param,transaction,commandTimeout,commandType);
        }

        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Connection.ExecuteScalarAsync<T>(sql,param,transaction,commandTimeout,commandType);
        }
    }
    //使dapper完美兼容匿名类型
    public sealed class LinqTypeMap : SqlMapper.ITypeMap
    {
        private readonly List<FieldInfo> _fields;
        private readonly Type _type;

        /// <summary>
        /// Creates default type map
        /// </summary>
        /// <param name="type">Entity type</param>
        public LinqTypeMap(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            _fields = GetSettableFields(type);
            Properties = GetSettableProps(type);
            _type = type;
        }
        internal static MethodInfo GetPropertySetter(PropertyInfo propertyInfo, Type type)
        {
            if (propertyInfo.DeclaringType == type) return propertyInfo.GetSetMethod(true);

            return propertyInfo.DeclaringType.GetProperty(
                   propertyInfo.Name,
                   BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                   Type.DefaultBinder,
                   propertyInfo.PropertyType,
                   propertyInfo.GetIndexParameters().Select(p => p.ParameterType).ToArray(),
                   null).GetSetMethod(true);
        }

        internal static List<PropertyInfo> GetSettableProps(Type t)
        {
            return t
                  .GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                  .Where(p => GetPropertySetter(p, t) != null)
                  .ToList();
        }

        internal static List<FieldInfo> GetSettableFields(Type t)
        {
            return t.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
        }

        /// <summary>
        /// Finds best constructor
        /// </summary>
        /// <param name="names">DataReader column names</param>
        /// <param name="types">DataReader column types</param>
        /// <returns>Matching constructor or default one</returns>
        public ConstructorInfo FindConstructor(string[] names, Type[] types)
        {
            var constructors = _type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (ConstructorInfo ctor in constructors.OrderBy(c => c.IsPublic ? 0 : (c.IsPrivate ? 2 : 1)).ThenBy(c => c.GetParameters().Length))
            {
                ParameterInfo[] ctorParameters = ctor.GetParameters();
                if (ctorParameters.Length == 0)
                    return ctor;

                if (ctorParameters.Length != types.Length)
                    continue;

                int i = 0;
                for (; i < ctorParameters.Length; i++)
                {
                    if (!string.Equals(ctorParameters[i].Name, names[i], StringComparison.OrdinalIgnoreCase))
                        break;
                    if (types[i] == typeof(byte[]) && ctorParameters[i].ParameterType.FullName == "System.Data.Linq.Binary")
                        continue;
                    var unboxedType = Nullable.GetUnderlyingType(ctorParameters[i].ParameterType) ?? ctorParameters[i].ParameterType;
                    //if ((unboxedType != types[i] && !SqlMapper.HasTypeHandler(unboxedType))
                    //    && !(unboxedType.IsEnum && Enum.GetUnderlyingType(unboxedType) == types[i])
                    //    && !(unboxedType == typeof(char) && types[i] == typeof(string))
                    //    && !(unboxedType.IsEnum && types[i] == typeof(string)))
                    //{
                    //    break;
                    //}
                }

                if (i == ctorParameters.Length)
                    return ctor;
            }

            return null;
        }

        /// <summary>
        /// Returns the constructor, if any, that has the ExplicitConstructorAttribute on it.
        /// </summary>
        public ConstructorInfo FindExplicitConstructor()
        {
            var constructors = _type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

            var withAttr = constructors.Where(c => c.GetCustomAttributes(typeof(ExplicitConstructorAttribute), true).Length > 0).ToList();

            if (withAttr.Count == 1)
            {
                return withAttr[0];
            }

            return null;
        }

        /// <summary>
        /// Gets mapping for constructor parameter
        /// </summary>
        /// <param name="constructor">Constructor to resolve</param>
        /// <param name="columnName">DataReader column name</param>
        /// <returns>Mapping implementation</returns>
        public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName)
        {
            var parameters = constructor.GetParameters();

            return new LinqMemberMap(columnName, parameters.FirstOrDefault(p => string.Equals(p.Name, columnName, StringComparison.OrdinalIgnoreCase)));
        }

        /// <summary>
        /// Gets member mapping for column
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <returns>Mapping implementation</returns>
        public SqlMapper.IMemberMap GetMember(string columnName)
        {
            var property = Properties.Find(p => string.Equals(p.Name, columnName, StringComparison.Ordinal))
               ?? Properties.Find(p => string.Equals(p.Name, columnName, StringComparison.OrdinalIgnoreCase));

            if (property == null && MatchNamesWithUnderscores)
            {
                property = Properties.Find(p => string.Equals(p.Name, columnName.Replace("_", ""), StringComparison.Ordinal))
                    ?? Properties.Find(p => string.Equals(p.Name, columnName.Replace("_", ""), StringComparison.OrdinalIgnoreCase));
            }

            if (property != null)
                return new LinqMemberMap(columnName, property);

            // roslyn automatically implemented properties, in particular for get-only properties: <{Name}>k__BackingField;
            var backingFieldName = "<" + columnName + ">k__BackingField";

            // preference order is:
            // exact match over underscre match, exact case over wrong case, backing fields over regular fields, match-inc-underscores over match-exc-underscores
            var field = _fields.Find(p => string.Equals(p.Name, columnName, StringComparison.Ordinal))
                ?? _fields.Find(p => string.Equals(p.Name, backingFieldName, StringComparison.Ordinal))
                ?? _fields.Find(p => string.Equals(p.Name, columnName, StringComparison.OrdinalIgnoreCase))
                ?? _fields.Find(p => string.Equals(p.Name, backingFieldName, StringComparison.OrdinalIgnoreCase));

            if (field == null && MatchNamesWithUnderscores)
            {
                var effectiveColumnName = columnName.Replace("_", "");
                backingFieldName = "<" + effectiveColumnName + ">k__BackingField";

                field = _fields.Find(p => string.Equals(p.Name, effectiveColumnName, StringComparison.Ordinal))
                    ?? _fields.Find(p => string.Equals(p.Name, backingFieldName, StringComparison.Ordinal))
                    ?? _fields.Find(p => string.Equals(p.Name, effectiveColumnName, StringComparison.OrdinalIgnoreCase))
                    ?? _fields.Find(p => string.Equals(p.Name, backingFieldName, StringComparison.OrdinalIgnoreCase));
            }

            if (field != null)
                return new LinqMemberMap(columnName, field);

            return null;
        }
        /// <summary>
        /// Should column names like User_Id be allowed to match properties/fields like UserId ?
        /// </summary>
        public static bool MatchNamesWithUnderscores { get; set; }

        /// <summary>
        /// The settable properties for this typemap
        /// </summary>
        public List<PropertyInfo> Properties { get; }
    }
    public sealed class LinqMemberMap : Dapper.SqlMapper.IMemberMap
    {
        /// <summary>
        /// Creates instance for simple property mapping
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <param name="property">Target property</param>
        public LinqMemberMap(string columnName, PropertyInfo property)
        {
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            Property = property ?? throw new ArgumentNullException(nameof(property));
        }

        /// <summary>
        /// Creates instance for simple field mapping
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <param name="field">Target property</param>
        public LinqMemberMap(string columnName, FieldInfo field)
        {
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            Field = field ?? throw new ArgumentNullException(nameof(field));
        }

        /// <summary>
        /// Creates instance for simple constructor parameter mapping
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <param name="parameter">Target constructor parameter</param>
        public LinqMemberMap(string columnName, ParameterInfo parameter)
        {
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            Parameter = parameter ?? throw new ArgumentNullException(nameof(parameter));
        }

        /// <summary>
        /// DataReader column name
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Target member type
        /// </summary>
        public Type MemberType => Field?.FieldType ?? Property?.PropertyType ?? Parameter?.ParameterType;

        /// <summary>
        /// Target property
        /// </summary>
        public PropertyInfo Property { get; }

        /// <summary>
        /// Target field
        /// </summary>
        public FieldInfo Field { get; }

        /// <summary>
        /// Target constructor parameter
        /// </summary>
        public ParameterInfo Parameter { get; }
    }
}
