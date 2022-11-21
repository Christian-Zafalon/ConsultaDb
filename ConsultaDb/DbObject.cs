using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsultaDb
{
    public class Tables
    {
        public string TableName { get; set; }

        public List<Column> Columns { get; set; }

    }

    public class Column
    {
        public string name { get; set; }
        public string type { get; set; }
        public string prec { get; set; }
        public string scale { get; set; }
        public string length { get; set; }
        public string nullable { get; set; }
        public int is_identity { get; set; }
       // public string collation { get; set; }
        public string type_constraints { get; set; }
        public string name_constraint { get; set; }
        public string referenced_object { get; set; }
        public string column_constraints { get; set; }
        public string name_default { get; set; }
        public string table_default { get; set; }
        public string column_default { get; set; }
        public string valor_default { get; set; }
    }


}
