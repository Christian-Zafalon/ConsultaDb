using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace ConsultaDb
{
    public class ColumnComparer : IEqualityComparer<Column>
    {
        public bool Equals(Column x, Column y)
        {
            return x.name == y.name && x.type_constraints == y.type_constraints;
        }

        public int GetHashCode([DisallowNull] Column obj)
        {
            return obj.name.GetHashCode();
        }
    }
}
