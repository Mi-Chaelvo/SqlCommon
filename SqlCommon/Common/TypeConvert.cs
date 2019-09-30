﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;

namespace SqlCommon
{
    /// <summary>
    /// DbColumn Information
    /// </summary>
    public class DbDataInfo
    {
        public string TypeName { get; set; }
        public Type DataType { get; set; }
        public string DataName { get; set; }
        public int Ordinal { get; set; }
        public DbDataInfo(int ordinal, string typeName, Type dataType, string dataName)
        {
            Ordinal = ordinal;
            TypeName = typeName;
            DataType = dataType;
            DataName = dataName;
        }
    }
    /// <summary>
    /// Type Convert
    /// </summary>
    public class TypeConvert
    {
        private static Dictionary<SerializerKey, object> _serializers = new Dictionary<SerializerKey, object>();
        private static Dictionary<Type, Func<object, Dictionary<string, object>>> _deserializers = new Dictionary<Type, Func<object, Dictionary<string, object>>>();
        private struct SerializerKey : IEquatable<SerializerKey>
        {
            private string[] Names { get; set; }
            private Type Type { get; set; }
            public override bool Equals(object obj)
            {
                return obj is SerializerKey && Equals((SerializerKey)obj);
            }
            public bool Equals(SerializerKey other)
            {
                if (Type != other.Type)
                {
                    return false;
                }
                else if (Names.Length != other.Names.Length)
                {
                    return false;
                }
                else
                {
                    for (int i = 0; i < Names.Length; i++)
                    {
                        if (Names[i] != other.Names[i])
                        {
                            return false;
                        }
                    }
                    return true;
                }
            }
            public override int GetHashCode()
            {
                return Type.GetHashCode();
            }
            public SerializerKey(Type type, string[] names)
            {
                Type = type;
                Names = names;
            }
        }
        /// <summary>
        /// IDataRecord Converted to T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="mapper">Type Mapper</param>
        /// <param name="record">IDataRecord</param>
        /// <returns></returns>
        public static Func<IDataRecord, T> GetSerializer<T>(ITypeMapper mapper, IDataRecord record)
        {
            string[] names = new string[record.FieldCount];
            for (int i = 0; i < record.FieldCount; i++)
            {
                names[i] = record.GetName(i);
            }
            var key = new SerializerKey(typeof(T), names);
            _serializers.TryGetValue(key, out object handler);
            if (handler == null)
            {
                lock (_serializers)
                {
                    handler = CreateTypeSerializerHandler<T>(mapper, record);
                    if (!_serializers.ContainsKey(key))
                    {
                        _serializers.Add(key, handler);
                    }
                }
            }
            return handler as Func<IDataRecord, T>;
        }
        /// <summary>
        /// IDataRecord Converted to T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="record"></param>
        /// <returns></returns>
        public static Func<IDataRecord, T> GetSerializer<T>(IDataRecord record)
        {
            return GetSerializer<T>(new TypeMapper(), record);
        }
        /// <summary>
        /// Object To Dictionary&lt;tstring, object&gt;
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Func<object, Dictionary<string, object>> Deserializer(object value)
        {
            Func<object, Dictionary<string, object>> handler = null;
            if (value is Dictionary<string, object>)
            {
                return (object param) => param as Dictionary<string, object>;
            }
            var type = value.GetType();
            _deserializers.TryGetValue(type, out handler);
            if (handler == null)
            {
                lock (_deserializers)
                {
                    handler = CreateTypeDeserializeHandler(type) as Func<object, Dictionary<string, object>>;
                    if (!_deserializers.ContainsKey(type))
                    {
                        _deserializers.Add(type, handler);
                    }
                }
            }
            return handler;
        }
        private static Func<object, Dictionary<string, object>> CreateTypeDeserializeHandler(Type type)
        {
            var properties = type.GetProperties();
            var dynamicMethod = new DynamicMethod($"Serializer{type.Name}", typeof(Dictionary<string, object>), new Type[] { typeof(object) }, type, true);
            var generator = dynamicMethod.GetILGenerator();
            LocalBuilder entityLocal = generator.DeclareLocal(typeof(Dictionary<string, object>));
            generator.Emit(OpCodes.Newobj, typeof(Dictionary<string, object>).GetConstructor(Type.EmptyTypes));
            generator.Emit(OpCodes.Stloc, entityLocal);

            foreach (var item in properties)
            {
                generator.Emit(OpCodes.Ldloc, entityLocal);
                generator.Emit(OpCodes.Ldstr, item.Name);
                generator.Emit(OpCodes.Ldarg_0);
                generator.Emit(OpCodes.Callvirt, item.GetGetMethod());
                if (item.PropertyType.IsValueType)
                {
                    generator.Emit(OpCodes.Box, item.PropertyType);
                }
                generator.Emit(OpCodes.Callvirt, typeof(Dictionary<string, object>).GetMethod(nameof(Dictionary<string, object>.Add), new Type[] { typeof(string), typeof(object) }));
            }

            generator.Emit(OpCodes.Ldloc, entityLocal);
            generator.Emit(OpCodes.Ret);
            return dynamicMethod.CreateDelegate(typeof(Func<object, Dictionary<string, object>>)) as Func<object, Dictionary<string, object>>;
        }
        private static Func<IDataRecord, T> CreateTypeSerializerHandler<T>(ITypeMapper mapper, IDataRecord record)
        {
            var type = typeof(T);
            var dynamicMethod = new DynamicMethod($"{type.Name}Deserializer{Guid.NewGuid().ToString("N")}", type, new Type[] { typeof(IDataRecord) }, type, true);
            var generator = dynamicMethod.GetILGenerator();
            LocalBuilder local = generator.DeclareLocal(type);
            var dataInfos = new DbDataInfo[record.FieldCount];
            for (int i = 0; i < record.FieldCount; i++)
            {
                var dataname = record.GetName(i);
                var datatype = record.GetFieldType(i);
                var typename = record.GetDataTypeName(i);
                dataInfos[i] = new DbDataInfo(i, typename, datatype, dataname);
            }
            if (dataInfos.Length == 1)
            {
                var convertMethod = mapper.FindConvertMethod(type);
                generator.Emit(OpCodes.Ldarg_0);
                generator.Emit(OpCodes.Ldc_I4, 0);
                if (convertMethod.IsVirtual)
                    generator.Emit(OpCodes.Callvirt, convertMethod);
                else
                    generator.Emit(OpCodes.Call, convertMethod);
                generator.Emit(OpCodes.Stloc, local);
                generator.Emit(OpCodes.Ldloc, local);
                generator.Emit(OpCodes.Ret);
                return dynamicMethod.CreateDelegate(typeof(Func<IDataRecord, T>)) as Func<IDataRecord, T>;
            }
            var constructor = mapper.FindConstructor(type);
            if (constructor.GetParameters().Length > 0)
            {
                var parameters = constructor.GetParameters();
                var locals = new LocalBuilder[parameters.Length];
                for (int i = 0; i < locals.Length; i++)
                {
                    locals[i] = generator.DeclareLocal(parameters[i].ParameterType);
                }
                for (int i = 0; i < locals.Length; i++)
                {
                    var item = mapper.FindConstructorParameter(dataInfos, parameters[i]);
                    if (item == null)
                    {
                        continue;
                    }
                    var convertMethod = mapper.FindConvertMethod(parameters[i].ParameterType);
                    generator.Emit(OpCodes.Ldarg_0);
                    generator.Emit(OpCodes.Ldc_I4, item.Ordinal);
                    if (convertMethod.IsVirtual)
                        generator.Emit(OpCodes.Callvirt, convertMethod);
                    else
                        generator.Emit(OpCodes.Call, convertMethod);
                    generator.Emit(OpCodes.Stloc, locals[i]);
                }
                for (int i = 0; i < locals.Length; i++)
                {
                    generator.Emit(OpCodes.Ldloc, locals[i]);
                }
                generator.Emit(OpCodes.Newobj, constructor);
                generator.Emit(OpCodes.Stloc, local);
                generator.Emit(OpCodes.Ldloc, local);
                generator.Emit(OpCodes.Ret);
                return dynamicMethod.CreateDelegate(typeof(Func<IDataRecord, T>)) as Func<IDataRecord, T>;
            }
            else
            {
                var properties = type.GetProperties();
                generator.Emit(OpCodes.Newobj, constructor);
                generator.Emit(OpCodes.Stloc, local);
                foreach (var item in dataInfos)
                {
                    var property = mapper.FindMember(properties, item) as PropertyInfo;
                    if (property == null)
                    {
                        continue;
                    }
                    var convertMethod = mapper.FindConvertMethod(property.PropertyType);
                    if (convertMethod == null)
                    {
                        continue;
                    }
                    int i = record.GetOrdinal(item.DataName);
                    generator.Emit(OpCodes.Ldloc, local);
                    generator.Emit(OpCodes.Ldarg_0);
                    generator.Emit(OpCodes.Ldc_I4, i);
                    if (convertMethod.IsVirtual)
                        generator.Emit(OpCodes.Callvirt, convertMethod);
                    else
                        generator.Emit(OpCodes.Call, convertMethod);
                    generator.Emit(OpCodes.Callvirt, property.GetSetMethod());
                }
                generator.Emit(OpCodes.Ldloc, local);
                generator.Emit(OpCodes.Ret);
                return dynamicMethod.CreateDelegate(typeof(Func<IDataRecord, T>)) as Func<IDataRecord, T>;
            }

        }
    }
}
