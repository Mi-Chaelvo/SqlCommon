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
            var connection = new System.Data.SqlClient.SqlConnection(@"Data Source=DESKTOP-9IS2HA6\SQLEXPRESS;Initial Catalog=test;Persist Security Info=True;User ID=sa;Password=1024");
            var db = new DbContext(new Dapper.Extensions.DapperSqlMapper(connection), DbContextType.SqlServer);
            var list = db.From<Student>().Page(2,2).SelectMany();
        }
    }
}