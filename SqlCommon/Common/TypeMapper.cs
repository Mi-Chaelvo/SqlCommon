using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SqlCommon
{
    /// <summary>
    /// TypeMapper Interface
    /// </summary>
    public interface ITypeMapper
    {
        MemberInfo FindMember(MemberInfo[] properties, DbDataInfo dataInfo);
        MethodInfo FindConvertMethod(Type csharpType);
        DbDataInfo FindConstructorParameter(DbDataInfo[] dataInfos, ParameterInfo parameterInfo);
        ConstructorInfo FindConstructor(Type csharpType);
    }
    /// <summary>
    /// Default TypeMapper
    /// </summary>
    public class TypeMapper : ITypeMapper
    {
        /// <summary>
        /// Find parametric constructors.
        /// If there is no default constructor, the constructor with the most parameters is returned.
        /// </summary>
        /// <param name="csharpType"></param>
        /// <returns></returns>
        public ConstructorInfo FindConstructor(Type csharpType)
        {
            var constructor = csharpType.GetConstructor(Type.EmptyTypes);
            if (constructor == null)
            {
                var constructors = csharpType.GetConstructors();
                constructor = constructors.Where(a => a.GetParameters().Length == constructors.Max(s => s.GetParameters().Length)).FirstOrDefault();
            }
            return constructor;
        }
        /// <summary>
        /// Returns field information based on parameter information
        /// </summary>
        /// <param name="dataInfos"></param>
        /// <param name="parameterInfo"></param>
        /// <returns></returns>
        public DbDataInfo FindConstructorParameter(DbDataInfo[] dataInfos, ParameterInfo parameterInfo)
        {
            foreach (var item in dataInfos)
            {
                if (item.DataName.Equals(parameterInfo.Name, StringComparison.OrdinalIgnoreCase))
                {
                    return item;
                }
                else if (SqlMapper.MatchNamesWithUnderscores && item.DataName.Replace("_", "").Equals(parameterInfo.Name, StringComparison.OrdinalIgnoreCase))
                {
                    return item;
                }
            }
            return null;
        }
        /// <summary>
        /// Returns attribute information based on field information
        /// </summary>
        /// <param name="properties"></param>
        /// <param name="dataInfo"></param>
        /// <returns></returns>
        public MemberInfo FindMember(MemberInfo[] properties, DbDataInfo dataInfo)
        {
            foreach (var item in properties)
            {
                if (item.Name.Equals(dataInfo.DataName, StringComparison.OrdinalIgnoreCase))
                {
                    return item;
                }
                else if (SqlMapper.MatchNamesWithUnderscores && item.Name.Equals(dataInfo.DataName.Replace("_", ""), StringComparison.OrdinalIgnoreCase))
                {
                    return item;
                }
            }
            return null;
        }
        /// <summary>
        /// Return type conversion function.
        /// </summary>
        /// <param name="csharpType"></param>
        /// <returns></returns>
        public MethodInfo FindConvertMethod(Type csharpType)
        {
            if (csharpType == typeof(byte) || Nullable.GetUnderlyingType(csharpType) == typeof(byte))
            {
                return csharpType == typeof(byte) ? DataConvertMethod.ToByteMethod : DataConvertMethod.ToByteNullableMethod;
            }
            if (csharpType == typeof(short) || Nullable.GetUnderlyingType(csharpType) == typeof(short))
            {
                return csharpType == typeof(short) ? DataConvertMethod.ToIn16Method : DataConvertMethod.ToIn16NullableMethod;
            }
            if (csharpType == typeof(int) || Nullable.GetUnderlyingType(csharpType) == typeof(int))
            {
                return csharpType == typeof(int) ? DataConvertMethod.ToIn32Method : DataConvertMethod.ToIn32NullableMethod;
            }
            if (csharpType == typeof(long) || Nullable.GetUnderlyingType(csharpType) == typeof(long))
            {
                return csharpType == typeof(long) ? DataConvertMethod.ToIn64Method : DataConvertMethod.ToIn64NullableMethod;
            }
            if (csharpType == typeof(float) || Nullable.GetUnderlyingType(csharpType) == typeof(float))
            {
                return csharpType == typeof(float) ? DataConvertMethod.ToFloatMethod : DataConvertMethod.ToFloatNullableMethod;
            }
            if (csharpType == typeof(double) || Nullable.GetUnderlyingType(csharpType) == typeof(double))
            {
                return csharpType == typeof(double) ? DataConvertMethod.ToDoubleMethod : DataConvertMethod.ToDoubleNullableMethod;
            }
            if (csharpType == typeof(decimal) || Nullable.GetUnderlyingType(csharpType) == typeof(decimal))
            {
                return csharpType == typeof(decimal) ? DataConvertMethod.ToDecimalMethod : DataConvertMethod.ToDecimalNullableMethod;
            }
            if (csharpType == typeof(bool) || Nullable.GetUnderlyingType(csharpType) == typeof(bool))
            {
                return csharpType == typeof(bool) ? DataConvertMethod.ToBooleanMethod : DataConvertMethod.ToBooleanNullableMethod;
            }
            if (csharpType == typeof(char) || Nullable.GetUnderlyingType(csharpType) == typeof(char))
            {
                return csharpType == typeof(char) ? DataConvertMethod.ToCharMethod : DataConvertMethod.ToCharNullableMethod;
            }
            if (csharpType == typeof(string))
            {
                return DataConvertMethod.ToStringMethod;
            }
            if (csharpType == typeof(DateTime) || Nullable.GetUnderlyingType(csharpType) == typeof(DateTime))
            {
                return csharpType == typeof(DateTime) ? DataConvertMethod.ToDateTimeMethod : DataConvertMethod.ToDateTimeNullableMethod;
            }
            if (csharpType.IsEnum || Nullable.GetUnderlyingType(csharpType).IsEnum)
            {
                return csharpType.IsEnum ? DataConvertMethod.ToEnumMethod.MakeGenericMethod(Nullable.GetUnderlyingType(csharpType)) : DataConvertMethod.ToEnumNullableMethod.MakeGenericMethod(Nullable.GetUnderlyingType(csharpType));
            }
            if (csharpType == typeof(Guid) || Nullable.GetUnderlyingType(csharpType) == typeof(Guid))
            {
                return csharpType == typeof(Guid) ? DataConvertMethod.ToGuidMethod : DataConvertMethod.ToGuidNullableMethod;
            }
            return null;
        }
    }
    public static class DataConvertMethod
    {
        #region Method Field
        public static MethodInfo ToByteMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToByte));
        public static MethodInfo ToIn16Method = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt16));
        public static MethodInfo ToIn32Method = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt32));
        public static MethodInfo ToIn64Method = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt64));
        public static MethodInfo ToFloatMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToFloat));
        public static MethodInfo ToDoubleMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDouble));
        public static MethodInfo ToDecimalMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDecimal));
        public static MethodInfo ToBooleanMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToBoolean));
        public static MethodInfo ToCharMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToChar));
        public static MethodInfo ToStringMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToString));
        public static MethodInfo ToDateTimeMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDateTime));
        public static MethodInfo ToEnumMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToEnum));
        public static MethodInfo ToGuidMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToGuid));
        #endregion

        #region NullableMethod Field
        public static MethodInfo ToByteNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt16Nullable));
        public static MethodInfo ToIn16NullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt16Nullable));
        public static MethodInfo ToIn32NullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt32Nullable));
        public static MethodInfo ToIn64NullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToInt64Nullable));
        public static MethodInfo ToFloatNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToFloatNullable));
        public static MethodInfo ToDoubleNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDoubleNullable));
        public static MethodInfo ToBooleanNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToBooleanNullable));
        public static MethodInfo ToDecimalNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDecimalNullable));
        public static MethodInfo ToCharNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToCharNullable));
        public static MethodInfo ToDateTimeNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToDateTimeNullable));
        public static MethodInfo ToEnumNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToEnumNullable));
        public static MethodInfo ToGuidNullableMethod = typeof(DataConvertMethod).GetMethod(nameof(DataConvertMethod.ConvertToGuidNullable));
        #endregion

        #region Define Convert
        public static byte ConvertToByte(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetByte(i);
            return result;
        }
        public static short ConvertToInt16(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt16(i);
            return result;
        }
        public static int ConvertToInt32(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt32(i);
            return result;
        }
        public static long ConvertToInt64(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt64(i);
            return result;
        }
        public static float ConvertToFloat(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetFloat(i);
            return result;
        }
        public static double ConvertToDouble(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDouble(i);
            return result;
        }
        public static bool ConvertToBoolean(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetBoolean(i);
            return result;
        }
        public static decimal ConvertToDecimal(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDecimal(i);
            return result;
        }
        public static char ConvertToChar(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetChar(i);
            return result;
        }
        public static string ConvertToString(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetString(i);
            return result;
        }
        public static DateTime ConvertToDateTime(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDateTime(i);
            return result;
        }
        public static T ConvertToEnum<T>(this IDataRecord dr, int i) where T : struct
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var value = dr.GetInt32(i);
            if (Enum.TryParse(value.ToString(), out T result)) return result;
            return default;
        }
        public static Guid ConvertToGuid(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetGuid(i);
            return result;
        }
        #endregion

        #region Define Nullable Convert
        public static byte? ConvertToByteNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetByte(i);
            return result;
        }
        public static short? ConvertToInt16Nullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt16(i);
            return result;
        }
        public static int? ConvertToInt32Nullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt32(i);
            return result;
        }
        public static long? ConvertToInt64Nullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetInt64(i);
            return result;
        }
        public static float? ConvertToFloatNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetFloat(i);
            return result;
        }
        public static double? ConvertToDoubleNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDouble(i);
            return result;
        }
        public static bool? ConvertToBooleanNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetBoolean(i);
            return result;
        }
        public static decimal? ConvertToDecimalNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDecimal(i);
            return result;
        }
        public static char? ConvertToCharNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetChar(i);
            return result;
        }
        public static DateTime? ConvertToDateTimeNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetDateTime(i);
            return result;
        }
        public static T? ConvertToEnumNullable<T>(this IDataRecord dr, int i) where T : struct
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var value = dr.GetInt32(i);
            if (Enum.TryParse(value.ToString(), out T result)) return result;
            return default;
        }
        public static Guid? ConvertToGuidNullable(this IDataRecord dr, int i)
        {
            if (dr.IsDBNull(i))
            {
                return default;
            }
            var result = dr.GetGuid(i);
            return result;
        }
        #endregion
    }
}
