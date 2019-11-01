using NUnit.Framework;
using System.Linq;
using SqlCommon;
using SqlCommon.Linq;
using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace SqlCommon.Tests
{
    public class PgsqlTest
    {
        [Test]
        public void TestMapper()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                db.Open();
                try
                {
                    var list = db.Connection.ExecuteQuery<Student>("select id as Id,name as Name from student where id in  @Id", new { Id = new int[] { 1, 2, 3 } }).ToList();
                }
                catch (Exception e)
                {

                    throw;
                }
            }
        }

        [Test]
        public void TestInsert()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                var row = 0;
                db.Open();//�Զ��ύ
                          //1.���������ֶΣ��������У�
                row = db.From<Student>().Insert(new Student()
                {
                    BirthDay = new DateTime(1996, 10, 20),
                    CreateTime = DateTime.Now,
                    IsDelete = false,
                    Name = "sqlcommon",
                    Score = Grade.C,
                    Version = Guid.NewGuid().ToString("N")
                });
                //2.���������ֶ�,����id���������У�
                var id = db.From<Student>().InsertReturnId(new Student()
                {
                    BirthDay = new DateTime(1996, 10, 20),
                    CreateTime = DateTime.Now,
                    IsDelete = false,
                    Score = Grade.A,
                    Name = "sqlcommon",
                    Version = Guid.NewGuid().ToString("N")
                });
                //3.����ָ���ֶ�
                row = db.From<Student>().Insert(s => new Student
                {
                    Score = Grade.A,
                    Name = "sqlcommon",
                });
                //4.�ų��ֶ�
                row = db.From<Student>()
                    .Filter(s => new { s.BirthDay, s.Score })
                    .Insert(new Student()
                    {
                        BirthDay = new DateTime(1996, 10, 20),
                        CreateTime = DateTime.Now,
                        IsDelete = false,
                        Name = "sqlcommon",
                        Score = Grade.C,
                        Version = Guid.NewGuid().ToString("N")
                    });
                //5.��������
                var students = new List<Student>();
                students.Add(new Student() { Name = "tom", Balance = 50 });
                students.Add(new Student() { Name = "bob", Balance = 100 });
                row = db.From<Student>().Insert(students);
            }
        }

        [Test]
        public void TestUpdate()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                var row = 0;
                db.Open();//�Զ��ύ
                var entity1 = new Student()
                {
                    Id = 22,
                    Balance = 50,
                    BirthDay = DateTime.Now,
                    Score = Grade.E,
                    IsDelete = true,
                    Name = "faker",
                    Version = "test",
                    CreateTime = DateTime.Now,
                };
                var entity2 = new Student()
                {
                    Id = 24,
                    Balance = 50,
                    BirthDay = DateTime.Now,
                    Score = Grade.E,
                    IsDelete = true,
                    Name = "faker",
                    Version = "test",
                    CreateTime = DateTime.Now,
                };
                //1.���������ֶθ���������
                row = db.From<Student>().Update(entity1);
                //2.����ָ����
                row = db.From<Student>().Filter(s => s.Version).Update(entity2);
                //3.����ָ����������������
                var oldVersion = "110";
                row = db.From<Student>()
                    .Where(a => a.Id == 25 && a.Version == oldVersion)
                    .Update(entity1);
                //4.�����޸�,���Ƽ�ʹ�ã�
                var students = new List<Student>();
                students.Add(entity1);
                students.Add(entity2);
                row = db.From<Student>().Filter(a => a.CreateTime).Update(students);
                //5.����ָ���ֶ�
                db.From<Student>()
                    .Update(s => new Student()
                    {
                        Id = 26,//Ĭ��ͨ��id���£�������where������Ĭ��
                        Score = Grade.F,
                        Name = "hihi"
                    });
                //6.��̬�����ֶ�
                try
                {
                    var charat = "f";
                    db.From<Student>()
                        .Set(a => a.Balance, 100, 1 > 2)//false
                        .Set(a => a.IsDelete, true)
                        .Set(a => a.Balance, a => a.Balance + SqlFun.MONEY(200), 1 < 2)//true
                        .Set(a => a.Name, a => SqlFun.Replace(a.Name, "b", charat))//true
                        .Where(a => a.Id == 27)
                        .Update();
                }
                catch (Exception e)
                {

                    throw;
                }
               

            }
        }

        [Test]
        public void TestDelete()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                db.Open();
                var row1 = db.From<Student>().Where(a => a.Id == 23).Delete();
                var row2 = db.From<Student>().Where(a => Operator.In(a.Id, new int[] { 1, 2, 3 })).Delete();
            }
        }

        [Test]
        public void TestSingle()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                db.Open();
                var entity1 = db.From<Student>().Single();
                var entity2 = db.From<Student>().OrderByDescending(a => a.Id).Single();
                var entity3 = db.From<Student>().OrderBy("create_time").Single();
                var balance = db.From<Student>().Where(a => a.Id == 24).Single(s => s.Balance);
                var entity4 = db.From<Student>().Where(a => a.Id == 24).Single(s => new
                {
                    s.Id,
                    s.Balance,
                    s.BirthDay,
                    s.IsDelete
                });
                var entity5 = db.From<Student>().Where(a => a.Id == 24).Single(s => new Student
                {
                    Id = s.Id,
                    Balance = s.Balance,
                    BirthDay = s.BirthDay,
                    IsDelete = s.IsDelete
                });
            }
        }

        [Test]
        public void TestSelect()
        {
            var connection = new Npgsql.NpgsqlConnection("Server=127.0.0.1;Port=5432;User Id=postgres;Password=1024;Database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Postgresql))
            {
                db.Open();
                try
                {
                    var list0 = db.From<Student>().Select(s => SqlFun.CONCAT(s.Name, s.Score)).ToList();
                    var list1 = db.From<Student>().Select().ToList();
                    var list2 = db.From<Student>().Where(a => a.IsDelete == false).Select().ToList();
                    var list3 = db.From<Student>().OrderBy(a => a.Id).OrderByDescending(a => a.Balance).Select().ToList();
                    var list4 = db.From<Student>().Take(4).Select().ToList();
                    var list5 = db.From<Student>().Take(4).Skip(2, 2).Select().ToList();
                    var list6 = db.From<Student>().Select(s => new { s.IsDelete, s.Id, s.Name }).ToList();
                    var list7 = db.From<Student>().Select(s => new Student { IsDelete = s.IsDelete, Id = s.Id, Name = s.Name }).ToList();
                }
                catch (Exception e)
                {

                    throw;
                }
               
            }
        }

        [Test]
        public void TestGroup()
        {
            var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
            {
                db.Open();
                var list = db.From<Student>()
                    .GroupBy(a => a.Name)
                    .Having(a => SqlFun.COUNT(1L) > 1)
                    .OrderByDescending(a => SqlFun.COUNT(1))
                    .Select(s => new
                    {
                        s.Name,
                        Names = SqlFun.GROUP_CONCAT(s.Name),
                        Count = SqlFun.COUNT(1L),
                        Balance = SqlFun.SUM(s.Balance)
                    }).ToList();
            }
        }

        [Test]
        public void TestPage()
        {
            var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
            {
                db.Open();

                //��һ�ַ�ʽ
                var list1 = db.From<Student>().Page(1, 2, out long total1).Select().ToList();
                var list2 = db.From<Student>().Page(2, 2, out long total2).Select().ToList();

                //�ڶ��ַ�ʽ
                var id = 24;
                var (list3, total3) = db.From<Student>().Where(a => a.Id > id).Page(1, 2).SelectMany();
                var (list4, total4) = db.From<Student>().Where(a => a.Id > id).Page(2, 2).SelectMany();

                //�����ҳ
                var (glist1, gtotal1) = db.From<Student>().GroupBy(a => a.Name).Page(1, 2).SelectMany(s => new { s.Name, Count = SqlFun.COUNT(1L) });
                var (glist2, gtotal2) = db.From<Student>().GroupBy(a => a.Name).Page(2, 2).SelectMany(s => new { s.Name, Count = SqlFun.COUNT(1L) });
            }
        }

        [Test]
        public void TestJoin()
        {
            var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
            using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
            {
                db.Open();
                var list0 = db.From<Student, Stuclass>()
                     .Join((a, b) => a.Id == b.Sid)
                     .OrderBy((a, b) => a.Id)
                     .OrderByDescending((a, b) => b.Id)
                     .Select((a, b) => new
                     {
                         a.Id,
                         a.Name,
                         Class = b.Name
                     }).ToList();
                var (list1, total1) = db.From<Student, Stuclass>()
                    .Join((a, b) => a.Id == b.Sid)
                    .Page(1, 2)
                    .SelectMany((a, b) => new
                    {
                        a.Id,
                        a.Name,
                        Class = b.Name
                    });
                var (list2, total2) = db.From<Student, Stuclass>()
                   .Join((a, b) => a.Id == b.Sid)
                   .GroupBy((a, b) => b.Name)
                   .Page(1, 2)
                   .SelectMany((a, b) => new
                   {
                       Class = b.Name,
                       StuNames = SqlFun.GROUP_CONCAT(a.Name)
                   });
                var list3 = db.From<Student, Stuclass, Stuid>()
                     .Join((Student a, Stuclass b) => a.Id == b.Sid)
                     .Join((Student a, Stuid b) => a.Id == b.Sid)
                     .Select((a, b, c) => new
                     {
                         a.Id,
                         b.Name,
                         c.IdNum
                     }).ToList();
            }
        }
    }
}