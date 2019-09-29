using NUnit.Framework;
using SqlCommon;
using System.Diagnostics;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System;
using MySql.Data.MySqlClient;

namespace NUnitTest
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void Test1()
        {
            try
            {

                var conenction = new MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
                //conenction.Open();
                //var cmd = conenction.CreateCommand();
                //cmd.CommandText = "select age as Id from student";
                //var reader = cmd.ExecuteReader();
                //while (reader.Read())
                //{
                //    var aa = reader.GetInt32(0);
                //}
                //for (int i = 0; i < 2; i++)
                //{
                //    var ccc = GetT((s)=>new 
                //    {
                //        s.NickName,
                //        s.IsDelete,
                //    }, conenction, "select name as NickName,is_delete as IsDelete from student").ToArray();
                //}
                
                var list = conenction.ExecuteQuery<Student>("select id as Id,is_delete as IsDelete,name as NickName from student where name like @ids",
                    new { ids = "%h%" }).ToList();
            }
            catch (System.Exception e)
            {

                throw;
            }
            Assert.Pass();
        }
        public static IEnumerable<Retult> GetT<Retult>(Func<Student, Retult> fun,IDbConnection connection, string sql)
        {
            return connection.ExecuteQuery<Retult>(sql);
        }
        public int GetInt32(IDataRecord record)
        {
            var result = DataConvertMethod.ConvertToInt32(record,0);
            return result;
        }
    }
    public class Student
    {
        public int Id { get; set; }
        public string NickName { get; set; }
        public int? Grade { get; set; }
        public bool? IsDelete { get; set; }
        public string Version { get; set; }
    }
}