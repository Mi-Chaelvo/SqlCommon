using System;
using System.Collections.Generic;
using System.Text;

namespace SqlCommon.Tests
{
    public static class SqlFun
    {
        [Linq.Function]
        public static string Replace(string column,string oldstr,string newstr)
        {
            return default;
        }
        [Linq.Function]
        public static decimal MONEY(decimal? column)
        {
            return default;
        }
        [Linq.Function]
        public static T COUNT<T>(T column)
        {
            return default;
        }
        [Linq.Function]
        public static T SUM<T>(T column)
        {
            return default;
        }
        [Linq.Function]
        public static T GROUP_CONCAT<T>(T expr)
        {
            return default;
        }
        [Linq.Function]
        public static object CONCAT(params object[] exprs)
        {
            return default;
        }
    }
}
