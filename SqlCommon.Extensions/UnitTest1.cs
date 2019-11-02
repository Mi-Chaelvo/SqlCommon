using NUnit.Framework;
using SqlCommon.Linq;

namespace SqlCommon.Extensions
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void DapperTest()
        {
            var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
            var db = new DbContext(new Dapper.Extensions.DapperSqlMapper(connection), DbContextType.Mysql);
            var list = db.From<Student>().Page(2,2).SelectMany();

        }
    }
}