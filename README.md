# SqlCommon
高性能ORM

## mapper

``` C#
//查询
var list = connection.ExecuteQuery<Stduent>("select id as Id,nick_name as NickNam from student where id=@Id",new { Id = 1 });

//修改
var row = connection.ExecuteQuery<Stduent>("insert into student(name,age) values (@Name,@Age)",new { Name = "admin",Age=10 });

//多结果集
var （list1,list2） = connection.ExecuteQuery<Stduent>("select * from student;select count(1) from student;");
var count = list2.First();
```

## expression

### insert

``` C#
 [Test]
 public void TestInsert()
 {
     //创建数据库上下文
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbContext(connection, DbContextType.Mysql))
     {
         var row = 0;
         db.Open();//自动提交
         db.From<Stuclass>().Single(s=>s.Id);
         db.From<Stuclass>().Single(s => s.Id);
         //1.更新所有字段（除自增列）
         row = db.From<Student>().Insert(new Student()
         {
             BirthDay = new DateTime(1996, 10, 20),
             CreateTime = DateTime.Now,
             IsDelete = false,
             Name = "sqlcommon",
             Score = Grade.C,
             Version = Guid.NewGuid().ToString("N")
         });
         //2.更新所有字段,返回id（除自增列）
         var id = db.From<Student>().InsertReturnId(new Student()
         {
             BirthDay = new DateTime(1996, 10, 20),
             CreateTime = DateTime.Now,
             IsDelete = false,
             Score = Grade.A,
             Name = "sqlcommon",
             Version = Guid.NewGuid().ToString("N")
         });
         //3.新增指定字段
         row = db.From<Student>().Insert(s => new Student
         {
             Score = Grade.A,
             Name = "sqlcommon",
         });
         //4.排除字段
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
         //5.批量新增
         var students = new List<Student>();
         students.Add(new Student() { Name = "tom", Balance = 50 });
         students.Add(new Student() { Name = "bob", Balance = 100 });
         row = db.From<Student>().Insert(students);
         Debug.WriteLine(string.Join("\r\n", db.Loggers.Select(s => s.Text)));
     }
 }
 
```
### update

``` C#
[Test]
 public void TestUpdate()
 {
     //创建数据库上下文-代理
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
     {
         var row = 0;
         db.Open();//自动提交
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
         //1.根据主键字段更新所有列
         row = db.From<Student>().Update(entity1);
         //2.忽略指定列
         row = db.From<Student>().Filter(s => s.Version).Update(entity2);
         //3.根据指定条件更新所有列
         var oldVersion = "110";
         row = db.From<Student>()
             .Where(a => a.Id == 25 && a.Version == oldVersion)
             .Update(entity1);
         //4.批量修改,不推荐使用，
         var students = new List<Student>();
         students.Add(entity1);
         students.Add(entity2);
         row = db.From<Student>().Filter(a => a.CreateTime).Update(students);
         //5.更新指定字段
         db.From<Student>()
             .Update(s => new Student()
             {
                 Id = 26,//默认通过id更新，可以用where来重置默认
                 Score = Grade.F,
                 Name = "hihi"
             });
         //6.动态更新字段
         var charat = "f";
         db.From<Student>()
             .Set(a => a.Balance, 100, 1 > 2)//false
             .Set(a => a.IsDelete, true)
             .Set(a => a.Balance, a => a.Balance + 200, 1 < 2)//true
             .Set(a => a.Name, a => SqlFun.Replace(a.Name, "b", charat))//true
             .Where(a => a.Id == 27)
             .Update();
         Debug.WriteLine(string.Join("\r\n", db.Loggers.Select(s => s.Text)));
     }
 }
 
```
### delete

``` C#
 [Test]
 public void TestDelete()
 {
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
     {
         db.Open();
         var row1 = db.From<Student>().Where(a => a.Id == 23).Delete();
         var row2 = db.From<Student>().Where(a => Operator.In(a.Id, new int[] { 1, 2, 3 })).Delete();
     }
 }
```

### Single

``` C#
 [Test]
 public void TestSingle()
 {
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
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
```
### select

``` C#
[Test]
 public void TestSelect()
 {
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
     {
         db.Open();
         var list0 = db.From<Student>().Select(s => SqlFun.CONCAT(s.Name, s.Score)).ToList();
         var list1 = db.From<Student>().Select().ToList();
         var list2 = db.From<Student>().Where(a => a.IsDelete == false).Select().ToList();
         var list3 = db.From<Student>().OrderBy(a => a.Id).OrderByDescending(a => a.Balance).Select().ToList();
         var list4 = db.From<Student>().Take(4).Select().ToList();
         var list5 = db.From<Student>().Take(4).Skip(2, 2).Select().ToList();
         var list6 = db.From<Student>().Select(s => new { s.IsDelete, s.Id, s.Name }).ToList();
         var list7 = db.From<Student>().Select(s => new Student { IsDelete = s.IsDelete, Id = s.Id, Name = s.Name }).ToList();
     }
 }
```

### groupby

``` C#
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
```
### page

``` C#
[Test]
 public void TestPage()
 {
     var connection = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user id=root;password=1024;database=test;");
     using (IDbContext db = new DbProxyContext(connection, DbContextType.Mysql))
     {
         db.Open();

         //第一种方式
         var list1 = db.From<Student>().Page(1, 2, out long total1).Select().ToList();
         var list2 = db.From<Student>().Page(2, 2, out long total2).Select().ToList();

         //第二种方式
         var id = 24;
         var (list3, total3) = db.From<Student>().Where(a => a.Id > id).Page(1, 2).SelectMany();
         var (list4, total4) = db.From<Student>().Where(a => a.Id > id).Page(2, 2).SelectMany();

         //分组分页
         var (glist1, gtotal1) = db.From<Student>().GroupBy(a => a.Name).Page(1, 2).SelectMany(s => new { s.Name, Count = SqlFun.COUNT(1L) });
         var (glist2, gtotal2) = db.From<Student>().GroupBy(a => a.Name).Page(2, 2).SelectMany(s => new { s.Name, Count = SqlFun.COUNT(1L) });
     }
 }
```

### join

``` C#
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
```

