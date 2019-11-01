using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SqlCommon.Linq
{
    public class SqlExpression : ExpressionVisitor
    {
        #region propertys
        public readonly StringBuilder SqlBuild = new StringBuilder();
        public Dictionary<string, object> Values { get; set; }
        private string ValueName = "Name";
        public string Prefix { get; set; }
        private string OperatorMethod { get; set; }
        private string Operator { get; set; }
        public bool SingleTable { get; set; }
        public DbContextType DbContextType { get; set; }
        #endregion

        #region override
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
                SetName(node);
            else
                SetValue(node);
            return node;
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Operator))
            {
                var notInclude = node.Method.Name == nameof(Linq.Operator.All)
                    || node.Method.Name == nameof(Linq.Operator.Any)
                    || node.Method.Name == nameof(Linq.Operator.Exists)
                    || node.Method.Name == nameof(Linq.Operator.NotExists);
                SqlBuild.AppendFormat("{0}", notInclude && node.Arguments.Count == 1 ? "" : "(");
                if (node.Arguments.Count == 1)
                {
                    if (notInclude)
                    {
                        SqlBuild.AppendFormat("{0}", Linq.Operator.GetOperator(node.Method.Name));
                        Visit(node.Arguments[0]);
                    }
                    else
                    {
                        Visit(node.Arguments[0]);
                        SqlBuild.AppendFormat(" {0} ", Linq.Operator.GetOperator(node.Method.Name));
                    }
                }
                else if (node.Arguments.Count == 2)
                {
                    Visit(node.Arguments[0]);
                    Operator = Linq.Operator.GetOperator(node.Method.Name);
                    OperatorMethod = node.Method.Name;
                    SqlBuild.AppendFormat(" {0} ", Operator);
                    Visit(node.Arguments[1]);
                }
                else
                {
                    Operator = Linq.Operator.GetOperator(node.Method.Name);
                    Visit(node.Arguments[0]);
                    SqlBuild.AppendFormat(" {0} ", Operator);
                    Visit(node.Arguments[1]);
                    SqlBuild.AppendFormat(" {0} ", Linq.Operator.GetOperator(ExpressionType.AndAlso));
                    Visit(node.Arguments[2]);
                }
                SqlBuild.AppendFormat("{0}", notInclude && node.Arguments.Count == 1 ? "" : ")");
            }
            else if (node.Method.GetCustomAttributes(typeof(FunctionAttribute), true).Length > 0)
            {
                SqlBuild.AppendFormat("{0}(", node.Method.Name.ToUpper());
                var parameters = node.Method.GetParameters();
                for (int i = 0; i < node.Arguments.Count; i++)
                {
                    if (node.Arguments[i] is NewArrayExpression newArrayExpression)
                    {
                        for (int j = 0; j < newArrayExpression.Expressions.Count; j++)
                        {
                            Visit(newArrayExpression.Expressions[j]);
                            if (j + 1 != newArrayExpression.Expressions.Count)
                                SqlBuild.Append(",");
                        }
                    }
                    else
                    {
                        Visit(node.Arguments[i]);
                    }
                    if (i + 1 != node.Arguments.Count)
                        SqlBuild.Append(",");
                }
                SqlBuild.Append(")");
            }
            else
            {
                SetValue(node);
            }
            return node;
        }
        protected override Expression VisitBinary(BinaryExpression node)
        {
            SqlBuild.Append("(");
            Visit(node.Left);
            if (node.Right is ConstantExpression && (node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual) && (node.Right as ConstantExpression).Value == null)
            {
                Operator = node.NodeType == ExpressionType.Equal ? Linq.Operator.GetOperator(nameof(Linq.Operator.IsNull)) : Linq.Operator.GetOperator(nameof(Linq.Operator.IsNotNull));
                SqlBuild.AppendFormat(" {0}", Operator);
            }
            else
            {
                Operator = Linq.Operator.GetOperator(node.NodeType);
                SqlBuild.AppendFormat(" {0} ", Operator);
                Visit(node.Right);
            }
            SqlBuild.Append(")");
            return node;
        }
        protected override Expression VisitNewArray(NewArrayExpression node)
        {
            SetValue(node);
            return node;
        }
        protected override Expression VisitNew(NewExpression node)
        {
            SetValue(node);
            return node;
        }
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                SqlBuild.AppendFormat("{0} ", Linq.Operator.GetOperator(ExpressionType.Not));
                Visit(node.Operand);
            }
            else
            {
                Visit(node.Operand);
            }
            return node;
        }
        protected override Expression VisitConstant(ConstantExpression node)
        {
            var value = ExpressionUtil.GetDbValue(node.Value, DbContextType);
            if (value == null)
            {
                SqlBuild.AppendFormat("NULL");
            }
            else if (Operator == "LIKE" || Operator == "NOT LIKE")
            {
                if (OperatorMethod == nameof(Linq.Operator.EndsWith) || OperatorMethod == nameof(Linq.Operator.NotEndsWith))
                {
                    SqlBuild.AppendFormat("'%{0}'", value);
                }
                else if (OperatorMethod == nameof(Linq.Operator.StartsWith) || OperatorMethod == nameof(Linq.Operator.NotStartsWith))
                {
                    SqlBuild.AppendFormat("'{0}%'", value);
                }
                else
                {
                    SqlBuild.AppendFormat("'%{0}%'", value);
                }
            }
            else if (value is string)
            {
                SqlBuild.AppendFormat("'{0}'", value);
            }
            else
            {
                SqlBuild.AppendFormat("{0}", value);
            }
            return node;
        }
        #endregion

        #region private
        private void SetName(MemberExpression expression)
        {
            var memberName = expression.Member.Name;
            var columnName = ExpressionUtil.GetColumnName(expression.Expression.Type, memberName, SingleTable);
            SqlBuild.Append(columnName);
            ValueName = memberName;
        }
        private void SetValue(Expression expression)
        {
            var value = ExpressionUtil.GetExpressionValue(expression);
            if (value is IQuery sqlBuilder)
            {
                SqlBuild.Append(sqlBuilder.Build(Values, DbContextType));
            }
            else
            {
                if (Operator == "LIKE" || Operator == "NOT LIKE")
                {
                    if (OperatorMethod == nameof(Linq.Operator.EndsWith) || OperatorMethod == nameof(Linq.Operator.NotEndsWith))
                    {
                        value = string.Format("%{0}", value);
                    }
                    else if (OperatorMethod == nameof(Linq.Operator.StartsWith) || OperatorMethod == nameof(Linq.Operator.NotStartsWith))
                    {
                        value = string.Format("{0}%", value);
                    }
                    else
                    {
                        value = string.Format("%{0}%", value);
                    }
                }
                value = ExpressionUtil.GetDbValue(value, DbContextType);
                var key = string.Format("{0}{1}{2}", Prefix, ValueName, Values.Count);
                Values.Add(key, value);
                SqlBuild.Append(key);
            }
        }
        #endregion
    }
    public class ExpressionUtil : ExpressionVisitor
    {
        public static string GetColumnName(Type type, string csharpName, bool singleTable)
        {
            var columnName = TableInfoCache.GetColumn(type, f => f.CSharpName == csharpName)?.ColumnName ?? csharpName;
            if (!singleTable)
            {
                var tableName = TableInfoCache.GetTable(type).TableName;
                columnName = string.Format("{0}.{1}", tableName, columnName);
            }
            return columnName;
        }
        #region public
        public static string BuildExpression(Expression expression, Dictionary<string, object> param, DbContextType contextType, bool singleTable = true)
        {
            var visitor = new SqlExpression
            {
                Values = param,
                SingleTable = singleTable,
                Prefix = GetPrefix(contextType),
                DbContextType=contextType,
            };
            visitor.Visit(expression);
            return visitor.SqlBuild.ToString();
        }
        public static List<KeyValuePair<string, string>> BuildNewOrInitExpression(Expression expression, Dictionary<string, object> param, DbContextType contextType, bool singleTable = true)
        {
            var columns = new List<KeyValuePair<string, string>>();
            if (expression is LambdaExpression)
            {
                expression = (expression as LambdaExpression).Body;
            }
            if (expression is MemberExpression memberExpression0 && memberExpression0.Expression != null && memberExpression0.Expression.NodeType == ExpressionType.Parameter)
            {
                var memberName = memberExpression0.Member.Name;
                var columnName = GetColumnName(memberExpression0.Expression.Type, memberExpression0.Member.Name, singleTable);
                columns.Add(new KeyValuePair<string, string>(memberName, columnName));
            }
            else if (expression is MemberInitExpression initExpression)
            {
                string columnExpression;
                for (int i = 0; i < initExpression.Bindings.Count; i++)
                {
                    var memberName = initExpression.Bindings[i].Member.Name;
                    var argument = (initExpression.Bindings[i] as MemberAssignment).Expression;
                    if (argument is UnaryExpression)
                    {
                        argument = (argument as UnaryExpression).Operand;
                    }
                    if (argument is MemberExpression memberExpression1 && memberExpression1.Expression != null && memberExpression1.Expression.NodeType == ExpressionType.Parameter)
                    {
                        columnExpression = GetColumnName(memberExpression1.Expression.Type, memberExpression1.Member.Name, singleTable);
                    }
                    else if (argument is MemberExpression memberExpression2 && memberExpression2.Expression != null && memberExpression2.Expression.NodeType == ExpressionType.Constant)
                    {
                        var value = GetExpressionValue(argument);
                        if (value is IQuery sqlBuilder)
                            columnExpression = sqlBuilder.Build(param, contextType);
                        else
                            columnExpression = value.ToString();
                    }
                    else if (argument is ConstantExpression)
                    {
                        columnExpression = GetExpressionValue(argument).ToString();
                    }
                    else
                    {
                        columnExpression = BuildExpression(argument, param, contextType, singleTable);
                    }
                    columns.Add(new KeyValuePair<string, string>(memberName, columnExpression));
                }
            }
            else if (expression is NewExpression newExpression)
            {
                string columnExpression;
                for (int i = 0; i < newExpression.Arguments.Count; i++)
                {
                    var memberName = newExpression.Members[i].Name;
                    var argument = newExpression.Arguments[i];
                    if (argument is UnaryExpression)
                    {
                        argument = (argument as UnaryExpression).Operand;
                    }
                    if (argument is MemberExpression memberExpression1 && memberExpression1.Expression != null && memberExpression1.Expression.NodeType == ExpressionType.Parameter)
                    {
                        columnExpression = GetColumnName(memberExpression1.Expression.Type, memberExpression1.Member.Name, singleTable);
                    }
                    else if (argument is MemberExpression memberExpression2 && memberExpression2.Expression != null && memberExpression2.Expression.NodeType == ExpressionType.Constant)
                    {
                        var value = GetExpressionValue(memberExpression2);
                        if (value is IQuery sqlBuilder)
                            columnExpression = sqlBuilder.Build(param, contextType);
                        else
                            columnExpression = value.ToString();
                    }
                    else if (argument is ConstantExpression)
                        columnExpression = GetExpressionValue(argument).ToString();
                    else
                    {
                        columnExpression = BuildExpression(argument, param, contextType, singleTable);
                    }
                    columns.Add(new KeyValuePair<string, string>(memberName, columnExpression));
                }
            }
            else
            {
                var memberName = string.Format("Expr");
                var columnName = BuildExpression(expression, param, contextType, singleTable);
                columns.Add(new KeyValuePair<string, string>(memberName, columnName));
            }
            return columns;
        }
        public static ColumnInfo BuildMemberExpression(Expression expression)
        {
            if (expression is LambdaExpression lambdaExpression)
            {
                expression = lambdaExpression.Body;
            }
            var memberExpression = expression as MemberExpression;
            var name = memberExpression.Member.Name;
            var type = memberExpression.Expression.Type;
            return TableInfoCache.GetColumn(type, f => f.CSharpName == name);
        }
        public static List<ColumnInfo> BuildNewExpression(Expression expression)
        {
            var list = new List<ColumnInfo>();
            expression = (expression as LambdaExpression).Body;
            if (expression is NewExpression newExpression)
            {
                for (int i = 0; i < newExpression.Arguments.Count; i++)
                {
                    var name = newExpression.Members[i].Name;
                    var type = newExpression.Members[i].DeclaringType;
                    var argument = newExpression.Arguments[i];
                    var columnInfo = TableInfoCache.GetColumn(type, f => f.CSharpName == name);
                    list.Add(columnInfo);
                }
            }
            else if (expression is MemberExpression)
            {
                list.Add(BuildMemberExpression(expression));
            }
            return list;
        }
        public static List<ColumnInfo> BuildMemberInitExpression(Expression expression, Dictionary<string, object> param, DbContextType contextType)
        {
            var list = new List<ColumnInfo>();
            expression = (expression as LambdaExpression).Body;
            var prefix = GetPrefix(contextType);
            if (expression is MemberInitExpression initExpression)
            {
                var type = initExpression.Type;
                for (int i = 0; i < initExpression.Bindings.Count; i++)
                {
                    Expression argument = (initExpression.Bindings[i] as MemberAssignment).Expression;
                    var name = initExpression.Bindings[i].Member.Name;
                    var columnInfo = TableInfoCache.GetColumn(type, f => f.CSharpName == name);
                    var value = GetExpressionValue(argument);
                    value = GetDbValue(value, contextType);
                    var key = string.Format("{0}{1}", prefix, name);
                    param.Add(key, value);
                    list.Add(columnInfo);
                }
            }
            return list;
        }
        public static object GetExpressionValue(Expression expression)
        {
            if ((expression is UnaryExpression unaryExpression) && unaryExpression.NodeType == ExpressionType.Convert)
            {
                expression = unaryExpression.Operand;
            }
            var names = new Stack<string>();
            var exps = new Stack<Expression>();
            var mifs = new Stack<System.Reflection.MemberInfo>();
            var tempExpression = expression;
            while (tempExpression is MemberExpression member)
            {
                names.Push(member.Member.Name);
                exps.Push(member.Expression);
                mifs.Push(member.Member);
                tempExpression = member.Expression;
            }
            if (names.Count > 0)
            {
                object value = null;
                foreach (var name in names)
                {
                    var exp = exps.Pop();
                    var mif = mifs.Pop();
                    if (exp is ConstantExpression cex)
                    {
                        value = cex.Value;
                    }
                    if (mif is System.Reflection.PropertyInfo pif)
                    {
                        value = pif.GetValue(value);
                    }
                    else if (mif is System.Reflection.FieldInfo fif)
                    {
                        value = fif.GetValue(value);
                    }
                }
                return value;
            }
            else if (expression is ConstantExpression constant)
            {
                return constant.Value;
            }
            else
            {
                return Expression.Lambda(expression).Compile().DynamicInvoke();
            }
        }
        public static object GetDbValue(object value, DbContextType contextType)
        {
            if (value == null)
            {
                return value;
            }
            if (value is Enum)
            {
                return Convert.ToInt32(value);
            }
            if (value is bool && contextType != DbContextType.Postgresql)
            {
                return Convert.ToBoolean(value) ? 1 : 0;
            }
            return value;
        }
        public static string GetPrefix(DbContextType contextType)
        {
            if (contextType == DbContextType.Oracle)
            {
                return ":";
            }
            else
            {
                return "@";
            }
        }
        #endregion
    }
    public static class Operator
    {
        #region extension
        public static bool In<T>(T column, IEnumerable<T> list)
        {
            return list.Contains(column);
        }
        public static bool In<T>(T column, params T[] values)
        {
            return values.Contains(column);
        }
        public static bool In<T>(T column, ISubQuery subuery)
        {
            subuery.GetHashCode();
            column.GetHashCode();
            return default;
        }
        public static bool NotIn<T>(T column, IEnumerable<T> enumerable)
        {
            return enumerable.Contains(column);
        }
        public static bool NotIn<T>(T column, params T[] values)
        {
            return values.Contains(column);
        }
        public static bool NotIn(ValueType column, ISubQuery subuery)
        {
            subuery.GetHashCode();
            column.GetHashCode();
            return default;
        }
        public static T Any<T>(T subquery) where T : ISubQuery
        {
            subquery.GetHashCode();
            return default;
        }
        public static T All<T>(T subquery) where T : ISubQuery
        {
            subquery.GetHashCode();
            return default;
        }
        public static bool Exists(ISubQuery subquery)
        {
            subquery.GetHashCode();
            return default;
        }
        public static bool NotExists(ISubQuery subquery)
        {
            subquery.GetHashCode();
            return default;
        }
        public static bool Contains(string column, string text)
        {
            return column.Contains(text);
        }
        public static bool NotContains(string column, string text)
        {
            return !column.Contains(text);
        }
        public static bool StartsWith(string column, string text)
        {
            return column.StartsWith(text);
        }
        public static bool NotStartsWith(string column, string text)
        {
            return !column.StartsWith(text);
        }
        public static bool EndsWith(string column, string text)
        {
            return column.EndsWith(text);
        }
        public static bool NotEndsWith(string column, string text)
        {
            return !column.EndsWith(text);
        }
        public static bool Regexp(string column, string regexp)
        {
            return System.Text.RegularExpressions.Regex.IsMatch(column, regexp);
        }
        public static bool NotRegexp(string column, string regexp)
        {
            return !Regexp(column, regexp);
        }
        public static bool IsNull<T>(T column)
        {
            return column == null;
        }
        public static bool IsNotNull<T>(T column)
        {
            return !IsNull(column);
        }
        public static bool Between<T>(T column, T value1, T value2)
        {
            column.GetHashCode();
            value1.GetHashCode();
            value2.GetHashCode();
            return default;
        }
        public static bool NotBetween<T>(T column, T value1, T value2)
        {
            column.GetHashCode();
            value1.GetHashCode();
            value2.GetHashCode();
            return default;
        }
        #endregion

        #region utils
        public static string GetOperator(string operatorType)
        {
            switch (operatorType)
            {
                case nameof(Operator.In):
                    operatorType = "IN";
                    break;
                case nameof(Operator.NotIn):
                    operatorType = "NOT IN";
                    break;
                case nameof(Operator.Any):
                    operatorType = "ANY";
                    break;
                case nameof(Operator.All):
                    operatorType = "ALL";
                    break;
                case nameof(Operator.Exists):
                    operatorType = "EXISTS";
                    break;
                case nameof(Operator.NotExists):
                    operatorType = "NOT EXISTS";
                    break;
                case nameof(Operator.Contains):
                case nameof(Operator.StartsWith):
                case nameof(Operator.EndsWith):
                    operatorType = "LIKE";
                    break;
                case nameof(Operator.NotContains):
                case nameof(Operator.NotStartsWith):
                case nameof(Operator.NotEndsWith):
                    operatorType = "NOT LIKE";
                    break;
                case nameof(Operator.IsNull):
                    operatorType = "IS NULL";
                    break;
                case nameof(Operator.IsNotNull):
                    operatorType = "IS NOT NULL";
                    break;
                case nameof(Operator.Between):
                    operatorType = "BETWEEN";
                    break;
                case nameof(Operator.NotBetween):
                    operatorType = "NOT BETWEEN";
                    break;
                case nameof(Operator.Regexp):
                    operatorType = "REGEXP";
                    break;
                case nameof(Operator.NotRegexp):
                    operatorType = "NOT REGEXP";
                    break;
            }
            return operatorType;
        }
        public static string GetOperator(ExpressionType type)
        {
            var condition = string.Empty;
            switch (type)
            {
                case ExpressionType.Add:
                    condition = "+";
                    break;
                case ExpressionType.Subtract:
                    condition = "-";
                    break;
                case ExpressionType.Multiply:
                    condition = "*";
                    break;
                case ExpressionType.Divide:
                    condition = "/";
                    break;
                case ExpressionType.Modulo:
                    condition = "%";
                    break;
                case ExpressionType.Equal:
                    condition = "=";
                    break;
                case ExpressionType.NotEqual:
                    condition = "<>";
                    break;
                case ExpressionType.GreaterThan:
                    condition = ">";
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    condition = ">=";
                    break;
                case ExpressionType.LessThan:
                    condition = "<";
                    break;
                case ExpressionType.LessThanOrEqual:
                    condition = "<=";
                    break;
                case ExpressionType.OrElse:
                    condition = "OR";
                    break;
                case ExpressionType.AndAlso:
                    condition = "AND";
                    break;
                case ExpressionType.Not:
                    condition = "NOT";
                    break;
            }
            return condition;
        }
        #endregion
    }
    public class TableInfoCache
    {
        private readonly static Dictionary<Type, TableInfo> _database = new Dictionary<Type, TableInfo>();
        private static TableInfo GetTableInfo(Type type)
        {
            _database.TryGetValue(type, out TableInfo table);
            if (table == null)
            {
                var properties = type.GetProperties();
                var columns = new List<ColumnInfo>();
                foreach (var item in properties)
                {
                    var columnName = item.Name;
                    var identity = false;
                    var columnKey = ColumnKey.None;
                    if (item.GetCustomAttributes(typeof(ColumnAttribute), true).Length > 0)
                    {
                        var columnAttribute = item.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault() as ColumnAttribute;
                        if (!columnAttribute.IsColumn)
                        {
                            continue;
                        }
                        columnKey = columnAttribute.Key;
                        identity = columnAttribute.IsIdentity;
                        columnName = string.IsNullOrEmpty(columnAttribute.Name) ? item.Name : columnAttribute.Name;
                    }
                    columns.Add(new ColumnInfo()
                    {
                        ColumnKey = columnKey,
                        ColumnName = columnName,
                        CSharpName = item.Name,
                        Identity = identity,
                    });
                }
                if (columns.Count > 0 && !columns.Exists(e => e.ColumnKey == ColumnKey.Primary))
                {
                    columns[0].ColumnKey = ColumnKey.Primary;
                }
                var tableName = type.Name;
                if (type.GetCustomAttributes(typeof(TableAttribute), true).Length > 0)
                {
                    tableName = (type.GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault() as TableAttribute).Name;
                }
                table = new TableInfo()
                {
                    CSharpName = type.Name,
                    CSharpType = type,
                    TableName = tableName,
                    Columns = columns,
                };
                lock (_database)
                {
                    if (!_database.ContainsKey(type))
                    {
                        _database.Add(type, table);
                    }
                }
            }
            return table;
        }
        public static TableInfo GetTable<T>() where T : class
        {
            return GetTableInfo(typeof(T));
        }
        public static TableInfo GetTable(Type type)
        {
            return GetTableInfo(type);
        }
        public static ColumnInfo GetColumn(Type type, Func<ColumnInfo, bool> func)
        {
            return GetTableInfo(type).Columns.Find(f => func(f));
        }
    }
    public class TableInfo
    {
        public Type CSharpType { get; set; }
        public string TableName { get; set; }
        public string CSharpName { get; set; }
        public List<ColumnInfo> Columns { get; set; }
    }
    public class ColumnInfo
    {
        public string ColumnName { get; set; }
        public string CSharpName { get; set; }
        public bool Identity { get; set; }
        public ColumnKey ColumnKey { get; set; }
        public override bool Equals(object obj)
        {
            return obj is ColumnInfo info &&
                   ColumnName == info.ColumnName;
        }
        public override int GetHashCode()
        {
            return -1862699260 + EqualityComparer<string>.Default.GetHashCode(ColumnName);
        }
    }
    public interface IQuery
    {
        string Build(Dictionary<string, object> values, DbContextType contextType);
    }
    public interface ISubQuery : IQuery
    {
    }
}
