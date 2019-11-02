using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SqlCommon.Linq
{
    /// <summary>
    /// Queryable
    /// </summary>
    public interface IQueryable<T>
    {
        /// <summary>
        /// Update column by subquery
        /// </summary>
        IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, ISubQuery subquery, bool condition = true);
        /// <summary>
        /// Update column by value
        /// </summary>
        IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, TResult value = default, bool condition = true);
        /// <summary>
        /// Update column by expression or function
        /// </summary>      
        IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Group by
        /// </summary>     
        IQueryable<T> GroupBy(string expression, bool condition = true);
        /// <summary>
        /// Group by
        /// </summary>     
        IQueryable<T> GroupBy<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Filter by sql
        /// </summary>     
        IQueryable<T> Where(string expression, bool condition = true);
        /// <summary>
        /// Filter by expression
        /// </summary>     
        IQueryable<T> Where<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Sort by sql
        /// </summary>
        IQueryable<T> OrderBy(string expression, bool condition = true);
        /// <summary>
        /// Ascending sort
        /// </summary>
        IQueryable<T> OrderBy<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Descending sort
        /// </summary>
        IQueryable<T> OrderByDescending<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Return "count" record from line "index"
        /// </summary>
        IQueryable<T> Skip(int index, int count, bool condition = true);
        /// <summary>
        /// Return "count" records from line 0
        /// </summary>
        IQueryable<T> Take(int count, bool condition = true);
        /// <summary>
        /// Return "count" records from page "index"
        /// </summary>   
        IQueryable<T> Page(int index, int count, bool condition = true);
        /// <summary>
        /// Return "count" records from page "index" and return the total
        /// </summary>      
        IQueryable<T> Page(int index, int count, out long total, bool condition = true);
        /// <summary>
        /// Filtering after grouping
        /// </summary>
        IQueryable<T> Having(string expression, bool condition = true);
        /// <summary>
        /// Filtering after grouping
        /// </summary>
        IQueryable<T> Having<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Filter field
        /// </summary>
        IQueryable<T> Filter<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Read lock
        /// </summary>
        IQueryable<T> With(string lockType, bool condition = true);
        /// <summary>
        /// Returns a single record
        /// </summary>
        T Single(string expression = null);
        /// <summary>
        /// Returns a single record
        /// </summary>
        Task<T> SingleAsync(string expression = null);
        /// <summary>
        /// Returns a single record
        /// </summary>
        TResult Single<TResult>(string expression = null);
        /// <summary>
        /// Returns a single record
        /// </summary>
        Task<TResult> SingleAsync<TResult>(string expression = null);
        /// <summary>
        /// Returns a single record
        /// </summary>
        TResult Single<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Returns single record
        /// </summary>
        Task<TResult> SingleAsync<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Returns list record
        /// </summary>
        IEnumerable<T> Select(string expression = null);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        (IEnumerable<T>, long) SelectMany(string expression = null);
        /// <summary>
        /// Returns list record
        /// </summary>
        Task<IEnumerable<T>> SelectAsync(string expression = null);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        Task<(IEnumerable<T>, long)> SelectManyAsync(string expression = null);
        /// <summary>
        /// Returns list record
        /// </summary>
        IEnumerable<TResult> Select<TResult>(string expression = null);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null);
        /// <summary>
        /// Returns list record
        /// </summary>
        Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null);
        /// <summary>
        /// Returns multiple record
        /// </summary>
        IEnumerable<TResult> Select<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Returns multiple record
        /// </summary>
        Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Returns list record and record total
        /// </summary>
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T, TResult>> expression);
        /// <summary>
        /// Insert a record
        /// </summary> 
        int Insert(T entity, bool condition = true);
        /// <summary>
        /// Insert a record
        /// </summary> 
        int Insert(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Insert a record
        /// </summary>
        Task<int> InsertAsync(T entity, bool condition = true);
        /// <summary>
        /// Insert a record
        /// </summary> 
        Task<int> InsertAsync(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Insert a record,and returns identity
        /// </summary> 
        long InsertReturnId(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Insert a record,and returns identity
        /// </summary> 
        Task<long> InsertReturnIdAsync(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Insert a record,and returns identity
        /// </summary>
        long InsertReturnId(T entity, bool condition = true);
        /// <summary>
        /// Insert a record,and returns identity
        /// </summary>
        Task<long> InsertReturnIdAsync(T entity, bool condition = true);
        /// <summary>
        /// Insert multiple record
        /// </summary>
        int Insert(IEnumerable<T> entitys, bool condition = true);
        /// <summary>
        /// Insert multiple record
        /// </summary>
        Task<int> InsertAsync(IEnumerable<T> entitys, bool condition = true);
        /// <summary>
        /// Execute update
        /// </summary>
        int Update(bool condition = true);
        /// <summary>
        /// Execute update
        /// </summary>
        Task<int> UpdateAsync(bool condition = true);
        /// <summary>
        /// Update a record
        /// </summary>
        int Update(T entity, bool condition = true);
        /// <summary>
        /// Update a record
        /// </summary>
        Task<int> UpdateAsync(T entity, bool condition = true);
        /// <summary>
        /// Update a record
        /// </summary>
        int Update(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Update a record
        /// </summary>
        Task<int> UpdateAsync(Expression<Func<T, T>> expression, bool condition = true);
        /// <summary>
        /// Update a record
        /// </summary>
        int Update(IEnumerable<T> entitys, bool condition = true);
        /// <summary>
        /// Update multiple record
        /// </summary>
        Task<int> UpdateAsync(IEnumerable<T> entitys, bool condition = true);
        /// <summary>
        /// Execute delete
        /// </summary>
        int Delete(bool condition = true);
        /// <summary>
        /// Execute delete
        /// </summary>
        Task<int> DeleteAsync(bool condition = true);
        /// <summary>
        /// Execute delete
        /// </summary>
        int Delete<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Execute delete
        /// </summary>
        Task<int> DeleteAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Existence query
        /// </summary>
        bool Exists(bool condition = true);
        /// <summary>
        /// Existence query
        /// </summary>
        Task<bool> ExistsAsync(bool condition = true);
        /// <summary>
        /// Existence query
        /// </summary>
        bool Exists<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Existence query
        /// </summary>
        Task<bool> ExistsAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Count query
        /// </summary>
        long Count(string expression = null, bool condition = true);
        /// <summary>
        /// Count query
        /// </summary>
        Task<long> CountAsync(string expression = null, bool condition = true);
        /// <summary>
        /// Count query
        /// </summary>
        long Count<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Count query
        /// </summary>
        Task<long> CountAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Summation query
        /// </summary>
        TResult Sum<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
        /// <summary>
        /// Summation query
        /// </summary>
        Task<TResult> SumAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true);
    }
    /// <summary>
    /// Queryable
    /// </summary>
    public interface IQueryable<T1, T2>
    {
        IQueryable<T1, T2> Join(string expression);
        IQueryable<T1, T2> Join(Expression<Func<T1, T2, bool?>> expression, JoinType join = JoinType.Inner);
        IQueryable<T1, T2> GroupBy(string expression, bool condition = true);
        IQueryable<T1, T2> GroupBy<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true);
        IQueryable<T1, T2> Where(string expression, bool condition = true);
        IQueryable<T1, T2> Where(Expression<Func<T1, T2, bool?>> expression, bool condition = true);
        IQueryable<T1, T2> OrderBy(string expression, bool condition = true);
        IQueryable<T1, T2> OrderBy<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true);
        IQueryable<T1, T2> OrderByDescending<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true);
        IQueryable<T1, T2> Skip(int index, int count, bool condition = true);
        IQueryable<T1, T2> Take(int count, bool condition = true);
        IQueryable<T1, T2> Page(int index, int count, bool condition = true);
        IQueryable<T1, T2> Page(int index, int count, out long total, bool condition = true);
        IQueryable<T1, T2> Having(string expression, bool condition = true);
        IQueryable<T1, T2> Having(Expression<Func<T1, T2, bool?>> expression, bool condition = true);
        IEnumerable<TResult> Select<TResult>(string expression = null);
        (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null);
        TResult Single<TResult>(string expression = null);
        Task<TResult> SingleAsync<TResult>(string expression = null);
        IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, TResult>> expression);
        (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, TResult>> expression);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, TResult>> expression);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, TResult>> expression);
        TResult Single<TResult>(Expression<Func<T1, T2, TResult>> expression);
        Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, TResult>> expression);
        long Count(string expression = null, bool condition = true);
        long Count<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true);
        Task<long> CountAsync(string expression = null, bool condition = true);
        Task<long> CountAsync<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true);
    }
    /// <summary>
    /// Queryable
    /// </summary>
    public interface IQueryable<T1, T2, T3>
    {
        IQueryable<T1, T2, T3> Join(string expression);
        IQueryable<T1, T2, T3> Join<V1, V2>(Expression<Func<V1, V2, bool?>> expression, JoinType join = JoinType.Inner) where V1 : class where V2 : class;
        IQueryable<T1, T2, T3> GroupBy(string expression, bool condition = true);
        IQueryable<T1, T2, T3> GroupBy<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3> Where(string expression, bool condition = true);
        IQueryable<T1, T2, T3> Where(Expression<Func<T1, T2, T3, bool?>> expression, bool condition = true);
        IQueryable<T1, T2, T3> OrderBy(string expression, bool condition = true);
        IQueryable<T1, T2, T3> OrderBy<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3> OrderByDescending<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3> Skip(int index, int count, bool condition = true);
        IQueryable<T1, T2, T3> Take(int count, bool condition = true);
        IQueryable<T1, T2, T3> Page(int index, int count, bool condition = true);
        IQueryable<T1, T2, T3> Page(int index, int count, out long total, bool condition = true);
        IQueryable<T1, T2, T3> Having(string expression, bool condition = true);
        IQueryable<T1, T2, T3> Having(Expression<Func<T1, T2, T3, bool?>> expression, bool condition = true);
        IEnumerable<TResult> Select<TResult>(string expression = null);
        (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null);
        TResult Single<TResult>(string expression = null);
        Task<TResult> SingleAsync<TResult>(string expression = null);
        IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        TResult Single<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression);
        long Count(string expression = null, bool condition = true);
        long Count<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true);
        Task<long> CountAsync(string expression = null, bool condition = true);
        Task<long> CountAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true);
    }
    /// <summary>
    /// Queryable
    /// </summary>
    public interface IQueryable<T1, T2, T3, T4>
    {
        IQueryable<T1, T2, T3, T4> Join(string expression);
        IQueryable<T1, T2, T3, T4> Join<V1, V2>(Expression<Func<V1, V2, bool?>> expression, JoinType join = JoinType.Inner) where V1 : class where V2 : class;
        IQueryable<T1, T2, T3, T4> GroupBy(string expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> GroupBy<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> Where(string expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> Where(Expression<Func<T1, T2, T3, T4, bool?>> expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> OrderBy(string expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> OrderBy<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> OrderByDescending<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> Skip(int index, int count, bool condition = true);
        IQueryable<T1, T2, T3, T4> Take(int count, bool condition = true);
        IQueryable<T1, T2, T3, T4> Page(int index, int count, bool condition = true);
        IQueryable<T1, T2, T3, T4> Page(int index, int count, out long total, bool condition = true);
        IQueryable<T1, T2, T3, T4> Having(string expression, bool condition = true);
        IQueryable<T1, T2, T3, T4> Having(Expression<Func<T1, T2, T3, T4, bool?>> expression, bool condition = true);
        IEnumerable<TResult> Select<TResult>(string expression = null);
        (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null);
        TResult Single<TResult>(string expression = null);
        Task<TResult> SingleAsync<TResult>(string expression = null);
        IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        TResult Single<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression);
        long Count(string expression = null, bool condition = true);
        long Count<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true);
        Task<long> CountAsync(string expression = null, bool condition = true);
        Task<long> CountAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true);
    }
    /// <summary>
    /// Join type
    /// </summary>
    public enum JoinType
    {
        Inner,
        Left,
        Right
    }
    /// <summary>
    /// OrderBy Expression
    /// </summary>
    public struct OrderByExpression
    {
        public bool? Asc { get; set; }
        public Expression Expression { get; set; }
        public OrderByExpression(bool asc, Expression expression)
        {
            Asc = asc;
            Expression = expression;
        }
        public OrderByExpression(Expression expression)
        {
            Asc = null;
            Expression = expression;
        }
    }
    /// <summary>
    /// Query common
    /// </summary>
    public abstract class BaseQuery
    {
        public string Prefix { get; internal set; } = "@";
        public int? RowIndex = null;
        public int? RowCount = null;
        public int? Timeout = null;
        public string ViewName = string.Empty;
        public IDbContext DbContext { get; set; }
        public DbContextType DbContextType => DbContext?.DbContextType ?? DbContextType.Mysql;
        public TableInfo TableInfo { get; set; }
        public bool SingleTable { get; set; }
        public string WithExpression = string.Empty;
        public List<Expression> SelectExpressions = new List<Expression>();
        public List<Expression> CountExpressions = new List<Expression>();
        public List<Expression> FilterExpressions = new List<Expression>();
        public List<Expression> WhereExpressions = new List<Expression>();
        public List<Expression> GroupByExpressions = new List<Expression>();
        public List<Expression> HavingExpressions = new List<Expression>();
        public List<OrderByExpression> OrderByExpressions = new List<OrderByExpression>();
        public Dictionary<string, object> Values = new Dictionary<string, object>();
        public string BuildCount()
        {
            var sql = new StringBuilder();
            var columnsql = BuildColumn(CountExpressions);
            var wheresql = BuildWhere(WhereExpressions);
            var groupsql = BuildGroupBy(GroupByExpressions);
            var havingsql = BuildHaving(HavingExpressions);
            columnsql = string.Format("{0}", string.IsNullOrEmpty(columnsql) ? "1" : columnsql);
            if (!string.IsNullOrEmpty(groupsql))
            {
                sql.AppendFormat("SELECT COUNT({0}) AS COUNT FROM (SELECT 1 AS COUNT FROM {1}", columnsql, ViewName);
                if (!string.IsNullOrEmpty(wheresql))
                {
                    sql.AppendFormat(" WHERE {0}", wheresql);
                }
                if (!string.IsNullOrEmpty(groupsql))
                {
                    sql.AppendFormat(" GROUP BY {0}", groupsql);
                }
                if (!string.IsNullOrEmpty(havingsql))
                {
                    sql.AppendFormat(" HAVING {0}", havingsql);
                }
                sql.Append(") AS T");
            }
            else
            {
                sql.AppendFormat("SELECT COUNT({0}) AS COUNT FROM {1}", columnsql, ViewName);
                if (!string.IsNullOrEmpty(wheresql))
                {
                    sql.AppendFormat(" WHERE {0}", wheresql);
                }
            }
            return sql.ToString();
        }
        public string BuildSelect()
        {
            Expression expression = SelectExpressions.First();
            var sql = new StringBuilder();
            var wheresql = BuildWhere(WhereExpressions);
            var groupsql = BuildGroupBy(GroupByExpressions);
            var havingsql = BuildHaving(HavingExpressions);
            var ordersql = BuildOrderBy(OrderByExpressions);
            var columnsql = BuildColumn(expression);
            if (DbContext?.DbContextType == DbContextType.SqlServer)
            {
                if (RowIndex == 0 && RowCount > 0)
                {
                    sql.AppendFormat("SELECT TOP {0} {1} FROM {2}", RowCount, columnsql, ViewName);
                    if (!string.IsNullOrEmpty(WithExpression))
                    {
                        sql.AppendFormat(" WITH ({0})", WithExpression);
                    }
                    if (!string.IsNullOrEmpty(wheresql))
                    {
                        sql.AppendFormat(" WHERE {0}", wheresql);
                    }
                    if (!string.IsNullOrEmpty(groupsql))
                    {
                        sql.AppendFormat(" GROUP BY {0}", groupsql);
                    }
                    if (!string.IsNullOrEmpty(havingsql))
                    {
                        sql.AppendFormat(" HAVING {0}", havingsql);
                    }
                    if (!string.IsNullOrEmpty(ordersql))
                    {
                        sql.AppendFormat(" ORDER BY {0}", ordersql);
                    }
                }
                else if (RowIndex > 0)
                {
                    ColumnInfo column = null;
                    if (TableInfo != null)
                    {
                        column = TableInfo.Columns.Find(f => f.ColumnKey == ColumnKey.Primary);
                    }
                    ordersql = !string.IsNullOrEmpty(ordersql)
                        ? ordersql : !string.IsNullOrEmpty(groupsql)
                        ? groupsql : column?.ColumnName;
                    ordersql = string.IsNullOrEmpty(ordersql) ? "RAND()" : ordersql;
                    sql.AppendFormat("SELECT TOP {0} * FROM (", RowCount);
                    var rownumsql = string.Format("ROW_NUMBER() OVER(ORDER BY {0})", ordersql);
                    sql.AppendFormat("SELECT {0},{1} AS RowNumber FROM {2}", columnsql, rownumsql, ViewName);
                    if (!string.IsNullOrEmpty(WithExpression) && string.IsNullOrEmpty(groupsql))
                    {
                        sql.AppendFormat(" WITH ({0})", WithExpression);
                    }
                    if (!string.IsNullOrEmpty(wheresql))
                    {
                        sql.AppendFormat(" WHERE {0}", wheresql);
                    }
                    if (!string.IsNullOrEmpty(groupsql))
                    {
                        sql.AppendFormat(" GROUP BY {0}", groupsql);
                    }
                    if (!string.IsNullOrEmpty(havingsql))
                    {
                        sql.AppendFormat(" HAVING {0}", havingsql);
                    }
                    sql.AppendFormat(") AS T WHERE RowNumber > {0}", RowIndex);
                }
                else
                {
                    sql.AppendFormat("SELECT {0} FROM {1}", columnsql, ViewName);
                    if (!string.IsNullOrEmpty(WithExpression))
                    {
                        sql.AppendFormat(" WITH ({0})", WithExpression);
                    }
                    if (!string.IsNullOrEmpty(wheresql))
                    {
                        sql.AppendFormat(" WHERE {0}", wheresql);
                    }
                    if (!string.IsNullOrEmpty(groupsql))
                    {
                        sql.AppendFormat(" GROUP BY {0}", groupsql);
                    }
                    if (!string.IsNullOrEmpty(havingsql))
                    {
                        sql.AppendFormat(" HAVING {0}", havingsql);
                    }
                    if (!string.IsNullOrEmpty(ordersql))
                    {
                        sql.AppendFormat(" ORDER BY {0}", ordersql);
                    }
                }
            }
            else
            {
                sql.AppendFormat("SELECT {0} FROM {1}", columnsql, ViewName);
                if (!string.IsNullOrEmpty(wheresql))
                {
                    sql.AppendFormat(" WHERE {0}", wheresql);
                }
                if (!string.IsNullOrEmpty(groupsql))
                {
                    sql.AppendFormat(" GROUP BY {0}", groupsql);
                }
                if (!string.IsNullOrEmpty(havingsql))
                {
                    sql.AppendFormat(" HAVING {0}", havingsql);
                }
                if (!string.IsNullOrEmpty(ordersql))
                {
                    sql.AppendFormat(" ORDER BY {0}", ordersql);
                }
                if (RowIndex != null && RowCount != null)
                {
                    sql.AppendFormat(" LIMIT {0} OFFSET {1}", RowCount, RowIndex);
                }
                if (!string.IsNullOrEmpty(WithExpression))
                {
                    sql.AppendFormat(" {0}", WithExpression);
                }
            }
            return sql.ToString();
        }
        public string BuildColumn(Expression expression)
        {
            if (expression is ConstantExpression constantExpression)
            {
                if (constantExpression.Value == null)
                {
                    var filter = BuildFilter(FilterExpressions);
                    var columns = TableInfo.Columns.Except(filter);
                    return string.Join(",", columns.Select(s => string.Format("{0} AS {1}", s.ColumnName, s.CSharpName)));
                }
                return constantExpression.Value.ToString();
            }
            else
            {
                var filter = BuildFilter(FilterExpressions);
                var columns = ExpressionUtil.BuildNewOrInitExpression(expression, Values, DbContextType, SingleTable);
                if (filter.Count > 0)
                {
                    columns = columns.Where(a => !filter.Exists(e => e.ColumnName == a.Value)).ToList();
                }
                return string.Join(",", columns.Select(s => string.Format("{0} AS {1}", s.Value, s.Key)));
            }
        }
        public string BuildColumn(List<Expression> expressions)
        {
            var list = new List<string>();
            foreach (var item in expressions)
            {
                if (item is ConstantExpression constantExpression)
                {
                    var value = constantExpression.Value;
                    if (value == null)
                    {
                        continue;
                    }
                    list.Add(value.ToString());
                }
                else
                {
                    var columns = ExpressionUtil.BuildNewOrInitExpression(item, Values, DbContextType, SingleTable);
                    list.AddRange(columns.Select(s => s.Value));
                }
            }
            return string.Join(",", list);
        }
        public static List<ColumnInfo> BuildFilter(List<Expression> expressions)
        {
            var list = new List<ColumnInfo>();
            foreach (var item in expressions)
            {
                if (item is ConstantExpression constantExpression)
                {
                    var value = constantExpression.Value;
                    if (value == null)
                    {
                        continue;
                    }
                }
                list.AddRange(ExpressionUtil.BuildNewExpression(item));
            }
            return list;
        }
        public string BuildWhere(List<Expression> expressions)
        {
            var sql = new StringBuilder();
            foreach (var item in expressions)
            {
                var result = string.Empty;
                if (item is ConstantExpression)
                {
                    var value = (item as ConstantExpression).Value;
                    if (result == null)
                    {
                        continue;
                    }
                    result = value.ToString();
                }
                else
                {
                    result = ExpressionUtil.BuildExpression(item, Values, DbContextType, SingleTable);
                }
                if (sql.Length > 0 && !string.IsNullOrEmpty(result))
                {
                    sql.AppendFormat(" {0} {1}", Operator.GetOperator(ExpressionType.AndAlso), result);
                }
                else
                {
                    sql.AppendFormat("{0}", result);
                }
            }
            return sql.ToString();
        }
        public string BuildGroupBy(List<Expression> expressions)
        {
            return BuildColumn(expressions);
        }
        public string BuildOrderBy(List<OrderByExpression> expressions)
        {
            var list = new List<string>();
            foreach (var item in expressions)
            {
                if (item.Expression is ConstantExpression constantExpression)
                {
                    var value = constantExpression.Value;
                    if (value == null)
                    {
                        continue;
                    }
                    list.Add(value.ToString());
                }
                else
                {
                    var columns = ExpressionUtil.BuildNewOrInitExpression(item.Expression, Values, DbContextType, SingleTable);
                    list.AddRange(columns.Select(s => string.Format("{0} {1}", s.Value, item.Asc == true ? "ASC" : "DESC")));
                }
            }
            return string.Join(",", list);
        }
        public string BuildHaving(List<Expression> expressions)
        {
            return BuildWhere(expressions);
        }
    }
    public class SqlQuery<T> : BaseQuery, IQueryable<T> where T : class
    {
        public SqlQuery()
        {

        }
        public SqlQuery(IDbContext dbContext = null, string viewName = null, int? timeout = null)
        {
            TableInfo = TableInfoCache.GetTable(typeof(T));
            DbContext = dbContext;
            ViewName = !string.IsNullOrEmpty(viewName) ? viewName : TableInfo.TableName;
            Prefix = ExpressionUtil.GetPrefix(dbContext?.DbContextType ?? DbContextType.Mysql);
            SingleTable = true;
            Timeout = timeout;
        }
        public List<Expression> SumExpressions = new List<Expression>();
        public List<KeyValuePair<Expression, object>> SetExpressions = new List<KeyValuePair<Expression, object>>();
        public void SetValue(object value)
        {
            if (value == null)
                return;
            var handler = TypeConvert.GetDeserializer(value.GetType());
            var values = handler(value);
            foreach (var item in values)
            {
                SetValue(item.Key, item.Value);
            }
        }
        public void SetValue(string key, object value)
        {
            value = ExpressionUtil.GetDbValue(value, DbContextType);
            if (Values.ContainsKey(key))
            {
                Values[key] = value;
            }
            else
            {
                Values.Add(key, value);
            }
        }
        public long Count(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                if (DbContext != null)
                {
                    var sql = BuildCount();
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public long Count<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildCount();
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<long> CountAsync(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                if (DbContext != null)
                {
                    var sql = BuildCount();
                    return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<long> CountAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildCount();
                    return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Delete(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildDelete();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Delete<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                Where(expression);
                if (DbContext != null)
                {
                    var sql = BuildDelete();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> DeleteAsync(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildDelete();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> DeleteAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                Where(expression);
                if (DbContext != null)
                {
                    var sql = BuildDelete();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public bool Exists<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                Where(expression);
                if (DbContext != null)
                {
                    var sql = BuildExists();
                    return DbContext.ExecuteScalar<bool>(sql, Values, Timeout);
                }
            }
            return false;
        }
        public async Task<bool> ExistsAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                Where(expression);
                if (DbContext != null)
                {
                    var sql = BuildExists();
                    return await DbContext.ExecuteScalarAsync<bool>(sql, Values, Timeout);
                }
            }
            return false;
        }
        public bool Exists(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildExists();
                    return DbContext.ExecuteScalar<bool>(sql, Values, Timeout);
                }
            }
            return false;
        }
        public async Task<bool> ExistsAsync(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildExists();
                    return await DbContext.ExecuteScalarAsync<bool>(sql, Values, Timeout);
                }
            }
            return false;
        }
        public IQueryable<T> Filter<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                FilterExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T> GroupBy(string expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T> GroupBy<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T> Having(string expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T> Having<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(expression);
            }
            return this;
        }
        public int Insert(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Insert(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Insert(IEnumerable<T> entitys, bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    var rows = 0;
                    foreach (var item in entitys)
                    {
                        Values.Clear();
                        SetValue(item);
                        rows += DbContext.ExecuteNonQuery(sql, Values, Timeout);
                    }
                    return rows;
                }
            }
            return 0;
        }
        public async Task<int> InsertAsync(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> InsertAsync(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> InsertAsync(IEnumerable<T> entitys, bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildInsert();
                    var rows = 0;
                    foreach (var item in entitys)
                    {
                        Values.Clear();
                        SetValue(item);
                        rows += await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                    }
                    return rows;
                }
            }
            return 0;
        }
        public long InsertReturnId(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildInsert(true);
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public long InsertReturnId(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildInsert(true);
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<long> InsertReturnIdAsync(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildInsert(true);
                    return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<long> InsertReturnIdAsync(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildInsert(true);
                    return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public IQueryable<T> OrderBy(string expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(Expression.Constant(expression)));
            }
            return this;
        }
        public IQueryable<T> OrderBy<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(true, expression));
            }
            return this;
        }
        public IQueryable<T> OrderByDescending<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(false, expression));
            }
            return this;
        }
        public IQueryable<T> Page(int index, int count, bool condition = true)
        {
            if (condition)
            {
                Skip(count * (index - 1), count);
            }
            return this;
        }
        public IQueryable<T> Page(int index, int count, out long total, bool condition = true)
        {
            total = 0;
            if (condition)
            {
                Page(index, count);
                total = Count();
            }
            return this;
        }
        public IEnumerable<T> Select(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<T>(sql, Values, Timeout);
            }
            return new List<T>();
        }
        public IEnumerable<TResult> Select<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public IEnumerable<TResult> Select<TResult>(Expression<Func<T, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<T>> SelectAsync(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<T>(sql, Values, Timeout);
            }
            return new List<T>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, ISubQuery subquery, bool condition = true)
        {
            if (condition)
            {
                SetExpressions.Add(new KeyValuePair<Expression, object>(column, subquery));
            }
            return this;
        }
        public (IEnumerable<T>, long) SelectMany(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<T, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<T>(), 0);
        }
        public async Task<(IEnumerable<T>, long)> SelectManyAsync(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<T, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<T>(), 0);
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, TResult value = default, bool condition = true)
        {
            if (condition)
            {
                SetExpressions.Add(new KeyValuePair<Expression, object>(column, value));
            }
            return this;
        }
        public IQueryable<T> Set<TResult>(Expression<Func<T, TResult>> column, Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                SetExpressions.Add(new KeyValuePair<Expression, object>(column, expression));
            }
            return this;
        }
        public T Single(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<T>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public TResult Single<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public TResult Single<TResult>(Expression<Func<T, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public async Task<T> SingleAsync(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<T>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public IQueryable<T> Skip(int index, int count, bool condition = true)
        {
            if (condition)
            {
                RowIndex = index;
                RowCount = count;
            }
            return this;
        }
        public TResult Sum<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                SumExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildSum();
                    return DbContext.ExecuteScalar<TResult>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public async Task<TResult> SumAsync<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                SumExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildSum();
                    return await DbContext.ExecuteScalarAsync<TResult>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public IQueryable<T> Take(int count, bool condition = true)
        {
            Skip(0, count, condition);
            return this;
        }
        public int Update(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null && SetExpressions.Count > 0)
                {
                    var sql = BuildUpdate();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Update(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Update(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    return DbContext.ExecuteNonQuery(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public int Update(IEnumerable<T> entitys, bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    var rows = 0;
                    foreach (var item in entitys)
                    {
                        Values.Clear();
                        SetValue(item);
                        rows += DbContext.ExecuteNonQuery(sql, Values, Timeout);
                    }
                    return rows;
                }
            }
            return 0;
        }
        public async Task<int> UpdateAsync(bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null && SetExpressions.Count > 0)
                {
                    var sql = BuildUpdate();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> UpdateAsync(T entity, bool condition = true)
        {
            if (condition)
            {
                SetValue(entity);
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> UpdateAsync(Expression<Func<T, T>> expression, bool condition = true)
        {
            if (condition)
            {
                SelectExpressions.Add(expression);
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    return await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                }
            }
            return 0;
        }
        public async Task<int> UpdateAsync(IEnumerable<T> entitys, bool condition = true)
        {
            if (condition)
            {
                if (DbContext != null)
                {
                    var sql = BuildUpdate();
                    var rows = 0;
                    foreach (var item in entitys)
                    {
                        Values.Clear();
                        SetValue(item);
                        rows += await DbContext.ExecuteNonQueryAsync(sql, Values, Timeout);
                    }
                    return rows;
                }
            }
            return 0;
        }
        public IQueryable<T> Where(string expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T> Where<TResult>(Expression<Func<T, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T> With(string lockType, bool condition = true)
        {
            if (condition)
            {
                WithExpression = lockType;
            }
            return this;
        }
        public string BuildInsert(bool returnId = false)
        {
            var sql = new StringBuilder();
            var filters = BuildFilter(FilterExpressions);
            sql.AppendFormat("INSERT INTO {0}", ViewName);
            if (SelectExpressions.Count > 0)
            {
                var columns = ExpressionUtil.BuildMemberInitExpression(SelectExpressions.First(), Values, DbContextType);
                columns = columns.Except(filters).ToList();
                sql.AppendFormat(" ({0})", string.Join(",", columns.Select(s => s.ColumnName)));
                sql.AppendFormat(" VALUES ({0})", string.Join(",", columns.Select(s => Prefix + s.CSharpName)));
            }
            else
            {
                var columns = TableInfo.Columns.Except(filters);
                columns = columns.Where(a => a.Identity == false);
                sql.AppendFormat(" ({0})", string.Join(",", columns.Select(s => s.ColumnName)));
                sql.AppendFormat(" VALUES ({0})", string.Join(",", columns.Select(s => Prefix + s.CSharpName)));
            }
            if (returnId)
            {
                if (DbContext?.DbContextType == DbContextType.Sqlite)
                {
                    sql.Append(";SELECT LAST_INSERT_ROWID() AS Id;");
                }
                else if (DbContext?.DbContextType == DbContextType.Mysql)
                {
                    sql.Append(";SELECT LAST_INSERT_ID() AS Id;");
                }
                else if (DbContext?.DbContextType == DbContextType.Postgresql)
                {
                    var column = TableInfo.Columns.Where(a => a.Identity == true).FirstOrDefault();
                    sql.AppendFormat("RETURNING {0}", column.ColumnName);
                }
                else
                {
                    sql.Append(";SELECT @@IDENTITY;");
                }
            }
            return sql.ToString();
        }
        public string BuildUpdate()
        {
            var sql = new StringBuilder();
            sql.AppendFormat("UPDATE {0} SET", ViewName);
            var filters = BuildFilter(FilterExpressions);
            if (SelectExpressions.Count > 0)
            {
                var columns = ExpressionUtil.BuildMemberInitExpression(SelectExpressions.First(), Values, DbContextType);
                columns = columns.Where(a => a.ColumnKey != ColumnKey.Primary).Except(filters).ToList();
                sql.AppendFormat(" {0}", string.Join(",", columns.Select(s => string.Format("{0} = {1}{2}", s.ColumnName, Prefix, s.CSharpName))));
            }
            else if (SetExpressions.Count > 0)
            {
                sql.Append(" ");
                for (var i = 0; i < SetExpressions.Count; i++)
                {
                    var item = SetExpressions[i];
                    var column = ExpressionUtil.BuildMemberExpression(item.Key);
                    if (item.Value is ISubQuery subquery)
                    {
                        sql.AppendFormat("{0} = {1}", column.ColumnName, subquery.Build(Values, DbContextType));
                    }
                    else if (item.Value is Expression expression)
                    {
                        sql.AppendFormat("{0} = {1}", column.ColumnName, ExpressionUtil.BuildExpression(expression, Values, DbContextType));
                    }
                    else
                    {
                        var key = string.Format("{0}{1}{2}", Prefix, column.CSharpName, Values.Count);
                        sql.AppendFormat("{0} = {1}", column.ColumnName, key);
                        SetValue(key, item.Value);
                    }
                    if (i + 1 < SetExpressions.Count)
                    {
                        sql.Append(",");
                    }
                }
            }
            else
            {
                var columns = TableInfo.Columns.Where(a => a.Identity == false).Except(filters);
                sql.AppendFormat(" {0}", string.Join(",", columns.Select(s => string.Format("{0} = {1}{2}", s.ColumnName, Prefix, s.CSharpName))));
            }
            var wheresql = BuildWhere(WhereExpressions);
            if (!string.IsNullOrEmpty(wheresql))
            {
                sql.AppendFormat(" WHERE {0}", wheresql);
            }
            else
            {
                var column = TableInfo.Columns.Where(a => a.ColumnKey == ColumnKey.Primary).FirstOrDefault();
                if (column != null && SetExpressions.Count == 0)
                {
                    sql.AppendFormat(" WHERE {0} = {1}{2}", column.ColumnName, Prefix, column.CSharpName);
                }
            }
            return sql.ToString();
        }
        public string BuildDelete()
        {
            var sql = new StringBuilder();
            sql.AppendFormat("DELETE FROM {0}", ViewName);
            var wheresql = BuildWhere(WhereExpressions);
            if (wheresql.Length > 0)
            {
                sql.AppendFormat(" WHERE {0}", wheresql);
            }
            return sql.ToString();
        }
        public string BuildSum()
        {
            var sql = new StringBuilder();
            var wheresql = BuildWhere(WhereExpressions);
            var columnsql = BuildColumn(SumExpressions);
            sql.AppendFormat("SELECT SUM({0}) AS SUM FROM {1}", columnsql, ViewName);
            if (!string.IsNullOrEmpty(wheresql))
            {
                sql.AppendFormat(" WHERE {0}", wheresql);
            }
            return sql.ToString();
        }
        public string BuildExists()
        {
            var sql = new StringBuilder();
            var wheresql = BuildWhere(WhereExpressions);
            var groupsql = BuildGroupBy(GroupByExpressions);
            var havingsql = BuildHaving(HavingExpressions);
            sql.AppendFormat("SELECT 1 WHERE EXISTS(SELECT 1 FROM {0}", ViewName);
            if (!string.IsNullOrEmpty(wheresql))
            {
                sql.AppendFormat(" WHERE {0}", wheresql);
            }
            if (!string.IsNullOrEmpty(groupsql))
            {
                sql.AppendFormat(" GROUP BY {0}", groupsql);
            }
            if (!string.IsNullOrEmpty(havingsql))
            {
                sql.AppendFormat(" HAVING {0}", havingsql);
            }
            sql.Append(")");
            return sql.ToString();
        }
    }
    public class SqlQuery<T1, T2> : BaseQuery, IQueryable<T1, T2> where T1 : class where T2 : class
    {
        public SqlQuery()
        {
        }
        public SqlQuery(IDbContext dbContext = null, string viewName = null, int? timeout = null)
        {
            TableInfo = null;
            DbContext = dbContext;
            Prefix = ExpressionUtil.GetPrefix(dbContext?.DbContextType ?? DbContextType.Mysql);
            SingleTable = false;
            Timeout = timeout;
            ViewName = viewName;
        }
        public List<string> TableNames = new List<string>();
        public long Count(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public long Count<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public async Task<long> CountAsync(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return default;
        }
        public async Task<long> CountAsync<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return 0;
        }
        public IQueryable<T1, T2> GroupBy(string expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2> GroupBy<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2> Having(string expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2> Having(Expression<Func<T1, T2, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2> Join(string expression)
        {
            ViewName = expression;
            return this;
        }
        public IQueryable<T1, T2> Join(Expression<Func<T1, T2, bool?>> expression, JoinType join = JoinType.Inner)
        {
            var onExpression = ExpressionUtil.BuildExpression(expression, Values, DbContextType, SingleTable);
            var table1Name = TableInfoCache.GetTable<T1>().TableName;
            var table2Name = TableInfoCache.GetTable<T2>().TableName;
            var joinType = string.Format("{0} JOIN", join.ToString().ToUpper());
            if (TableNames.Count == 0)
            {
                TableNames.Add(table1Name);
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} {2} ON {3}", table1Name, joinType, table2Name, onExpression));
            }
            else if (TableNames.Exists(a => table1Name == a))
            {
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table2Name, onExpression));
            }
            else
            {
                TableNames.Add(table1Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table1Name, onExpression));
            }
            return this;
        }
        public IQueryable<T1, T2> OrderBy(string expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(Expression.Constant(expression)));
            }
            return this;
        }
        public IQueryable<T1, T2> OrderBy<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(true, expression));
            }
            return this;
        }
        public IQueryable<T1, T2> OrderByDescending<TResult>(Expression<Func<T1, T2, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(false, expression));
            }
            return this;
        }
        public IQueryable<T1, T2> Page(int index, int count, out long total, bool condition = true)
        {
            total = 0;
            if (condition)
            {
                Page(index, count);
                total = Count();
            }
            return this;
        }
        public IQueryable<T1, T2> Page(int index, int count, bool condition = true)
        {
            if (condition)
            {
                Skip(count * (index - 1), count);
            }
            return this;
        }
        public IEnumerable<TResult> Select<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public TResult Single<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public TResult Single<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public IQueryable<T1, T2> Skip(int index, int count, bool condition = true)
        {
            if (condition)
            {
                RowIndex = index;
                RowCount = count;
            }
            return this;
        }
        public IQueryable<T1, T2> Take(int count, bool condition = true)
        {
            return Skip(0, count, condition);
        }
        public IQueryable<T1, T2> Where(string expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2> Where(Expression<Func<T1, T2, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(expression);
            }
            return this;
        }
    }
    public class SqlQuery<T1, T2, T3> : BaseQuery, IQueryable<T1, T2, T3> where T1 : class where T2 : class where T3 : class
    {
        public SqlQuery()
        {

        }
        public SqlQuery(IDbContext dbContext = null, string viewName = null, int? timeout = null)
        {
            TableInfo = null;
            DbContext = dbContext;
            Prefix = ExpressionUtil.GetPrefix(dbContext?.DbContextType ?? DbContextType.Mysql);
            SingleTable = false;
            Timeout = timeout;
            ViewName = viewName;
        }
        public List<string> TableNames = new List<string>();
        public long Count(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public long Count<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public async Task<long> CountAsync(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return default;
        }
        public async Task<long> CountAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return 0;
        }
        public IQueryable<T1, T2, T3> GroupBy(string expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> GroupBy<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Having(string expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Having(Expression<Func<T1, T2, T3, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Join(string expression)
        {
            if (string.IsNullOrEmpty(ViewName))
            {
                ViewName = expression;
            }
            else
            {
                ViewName = string.Format("{0} {1}", ViewName, expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Join<V1, V2>(Expression<Func<V1, V2, bool?>> expression, JoinType join = JoinType.Inner) where V1 : class where V2 : class
        {
            var onExpression = ExpressionUtil.BuildExpression(expression, Values, DbContextType, SingleTable);
            var table1Name = TableInfoCache.GetTable<V1>().TableName;
            var table2Name = TableInfoCache.GetTable<V2>().TableName;
            var joinType = string.Format("{0} JOIN", join.ToString().ToUpper());
            if (TableNames.Count == 0)
            {
                TableNames.Add(table1Name);
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} {2} ON {3}", table1Name, joinType, table2Name, onExpression));
            }
            else if (TableNames.Exists(a => table1Name == a))
            {
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table2Name, onExpression));
            }
            else
            {
                TableNames.Add(table1Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table1Name, onExpression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> OrderBy(string expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(Expression.Constant(expression)));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> OrderBy<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(true, expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> OrderByDescending<TResult>(Expression<Func<T1, T2, T3, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(false, expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Page(int index, int count, out long total, bool condition = true)
        {
            total = 0;
            if (condition)
            {
                Page(index, count);
                total = Count();
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Page(int index, int count, bool condition = true)
        {
            if (condition)
            {
                Skip(count * (index - 1), count);
            }
            return this;
        }
        public IEnumerable<TResult> Select<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public TResult Single<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public TResult Single<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, T3, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public IQueryable<T1, T2, T3> Skip(int index, int count, bool condition = true)
        {
            if (condition)
            {
                RowIndex = index;
                RowCount = count;
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Take(int count, bool condition = true)
        {
            return Skip(0, count, condition);
        }
        public IQueryable<T1, T2, T3> Where(string expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3> Where(Expression<Func<T1, T2, T3, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(expression);
            }
            return this;
        }
    }
    public class SqlQuery<T1, T2, T3, T4> : BaseQuery, IQueryable<T1, T2, T3, T4> where T1 : class where T2 : class where T3 : class where T4 : class
    {
        public SqlQuery()
        {

        }
        public SqlQuery(IDbContext dbContext = null, string viewName = null, int? timeout = null)
        {
            TableInfo = null;
            DbContext = dbContext;
            Prefix = ExpressionUtil.GetPrefix(dbContext?.DbContextType ?? DbContextType.Mysql);
            SingleTable = false;
            Timeout = timeout;
            ViewName = viewName;
        }
        public List<string> TableNames = new List<string>();
        public long Count(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public long Count<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                if (DbContext != null)
                {
                    return DbContext.ExecuteScalar<long>(sql, Values, Timeout);
                }
            }
            return default;
        }
        public async Task<long> CountAsync(string expression = null, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(Expression.Constant(expression));
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return default;
        }
        public async Task<long> CountAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                CountExpressions.Add(expression);
                var sql = BuildCount();
                return await DbContext.ExecuteScalarAsync<long>(sql, Values, Timeout);
            }
            return 0;
        }
        public IQueryable<T1, T2, T3, T4> GroupBy(string expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> GroupBy<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                GroupByExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Having(string expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Having(Expression<Func<T1, T2, T3, T4, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                HavingExpressions.Add(expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Join(string expression)
        {
            if (string.IsNullOrEmpty(ViewName))
            {
                ViewName = expression;
            }
            else
            {
                ViewName = string.Format("{0} {1}", ViewName, expression);
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Join<V1, V2>(Expression<Func<V1, V2, bool?>> expression, JoinType join = JoinType.Inner) where V1 : class where V2 : class
        {
            var onExpression = ExpressionUtil.BuildExpression(expression, Values, DbContextType, SingleTable);
            var table1Name = TableInfoCache.GetTable<V1>().TableName;
            var table2Name = TableInfoCache.GetTable<V2>().TableName;
            var joinType = string.Format("{0} JOIN", join.ToString().ToUpper());
            if (TableNames.Count == 0)
            {
                TableNames.Add(table1Name);
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} {2} ON {3}", table1Name, joinType, table2Name, onExpression));
            }
            else if (TableNames.Exists(a => table1Name == a))
            {
                TableNames.Add(table2Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table2Name, onExpression));
            }
            else
            {
                TableNames.Add(table1Name);
                Join(string.Format("{0} {1} ON {2}", joinType, table1Name, onExpression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> OrderBy(string expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(Expression.Constant(expression)));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> OrderBy<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(true, expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> OrderByDescending<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression, bool condition = true)
        {
            if (condition)
            {
                OrderByExpressions.Add(new OrderByExpression(false, expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Page(int index, int count, out long total, bool condition = true)
        {
            total = 0;
            if (condition)
            {
                Page(index, count);
                total = Count();
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Page(int index, int count, bool condition = true)
        {
            if (condition)
            {
                Skip(count * (index - 1), count);
            }
            return this;
        }
        public IEnumerable<TResult> Select<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public IEnumerable<TResult> Select<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public async Task<IEnumerable<TResult>> SelectAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout);
            }
            return new List<TResult>();
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public (IEnumerable<TResult>, long) SelectMany<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = DbContext.ExecuteQuery<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(string expression = null)
        {
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public async Task<(IEnumerable<TResult>, long)> SelectManyAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var countSql = BuildCount();
                var querySql = BuildSelect();
                var sql = string.Format("{0};{1}", querySql, countSql);
                var (item1, item2) = await DbContext.ExecuteQueryAsync<TResult, long>(sql, Values, Timeout);
                return (item1, item2.FirstOrDefault());
            }
            return (new List<TResult>(), 0);
        }
        public TResult Single<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public TResult Single<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return DbContext.ExecuteQuery<TResult>(sql, Values, Timeout).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(string expression = null)
        {
            Take(1);
            SelectExpressions.Add(Expression.Constant(expression));
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public async Task<TResult> SingleAsync<TResult>(Expression<Func<T1, T2, T3, T4, TResult>> expression)
        {
            Take(1);
            SelectExpressions.Add(expression);
            if (DbContext != null)
            {
                var sql = BuildSelect();
                return (await DbContext.ExecuteQueryAsync<TResult>(sql, Values, Timeout)).FirstOrDefault();
            }
            return default;
        }
        public IQueryable<T1, T2, T3, T4> Skip(int index, int count, bool condition = true)
        {
            if (condition)
            {
                RowIndex = index;
                RowCount = count;
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Take(int count, bool condition = true)
        {
            return Skip(0, count, condition);
        }
        public IQueryable<T1, T2, T3, T4> Where(string expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(Expression.Constant(expression));
            }
            return this;
        }
        public IQueryable<T1, T2, T3, T4> Where(Expression<Func<T1, T2, T3, T4, bool?>> expression, bool condition = true)
        {
            if (condition)
            {
                WhereExpressions.Add(expression);
            }
            return this;
        }
    }
}
