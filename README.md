# SqlCommon
高性能ORM

``` C#
var list = connection.ExecuteQuery<Stduent>("select id as Id,nick_name as NickNam from student where id=@Id",new { Id = 1 });
```
