using SqlCommon.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace SqlCommon.Tests
{
    public enum Grade
    {
        A=0,
        B=1,
        C=2,
        D=3,
        E=4,
        F=5
    }

    public class Student
    {
        [Column(IsIdentity = true)]
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime? BirthDay { get; set; }
        public Grade? Score { get; set; }
        public decimal? Balance { get; set; }
        [Column("is_delete")]
        public bool? IsDelete { get; set; }
        public string Version { get; set; }
        [Column("create_time")]
        public DateTime? CreateTime { get; set; }
    }
    public class Stuclass
    {
        public int? Id { get; set; }
        public int? Sid { get; set; }
        public string Name { get; set; }
    }
    public class Stuid
    {
        public int Id { get; set; }
        public int Sid { get; set; }
        [Column("id_num")]
        public int IdNum { get; set; }
    }
}
