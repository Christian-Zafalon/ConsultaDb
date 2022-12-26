using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace ConsultaDb
{
    class Program
    {
        //Criação das pastas
        public static string rootFolder = @"c:\temporario\";
        public static string tablesPath = Path.Combine(rootFolder, "tables");
        public static string ABRIRPASTA = Path.Join(tablesPath);


        static void Main(string[] args)
        {
            // Chamando o metodo de criar as pastas passando a pasta...
            CreateDirectory(tablesPath);

            #region Console version
            Console.ForegroundColor
                    = ConsoleColor.Blue;
            Console.WriteLine("X---------------------------X", Console.ForegroundColor);
            Console.ResetColor();
            Console.WriteLine("| V1 CONSULTA EM TABELA    |");
            Console.WriteLine("| V1 CONSULTA EM COLUNAS   |");
            Console.ForegroundColor
                    = ConsoleColor.Blue;
            Console.WriteLine("X---------------------------X", Console.ForegroundColor);
            Console.ResetColor();
            #endregion

            //Conexão BD
            const string connectionString = "Data Source=10.100.2.9; Initial Catalog=dbComlink_off_dev_linx; Integrated Security=SSPI;";

            //const string connectionString = "Data Source=CLK-NOTE_63; Initial Catalog=CLONE; Integrated Security=SSPI;";

            Console.WriteLine("Validando conexão");
            var sqlCon = new SqlConnection(connectionString);

            try
            {
                Console.WriteLine("Iniciando conexão ...");
                sqlCon.Open();
                //Alterar cor do console
                Console.ForegroundColor
                    = ConsoleColor.Green;

                Console.WriteLine("Conectado com sucesso!",
                                  Console.ForegroundColor);
                Console.ResetColor();
            }
            catch (Exception e)
            {
                Console.WriteLine("Erro ao se conectar ao ao banco: " + e.Message);
            }
            sqlCon.Close();
            CriandoUmaListaBaseadaEmConsultas(connectionString);
        }

        private static void CriandoUmaListaBaseadaEmConsultas(string connectionString)
        {
            using (var sqlCon = new SqlConnection(connectionString))
            {
                #region TRAZ as tabelas
                string sqlTables = $@" SELECT NAME TableName FROM SYS.TABLES";
                #endregion
                List<Tables> tables = sqlCon.Query<Tables>(sqlTables).ToList();

                #region TRAZ as colunas
                tables.ForEach(t =>
                {
                    string columnTables = $@"	DROP TABLE IF EXISTS #TEMP_DEFAULT
                                                DROP TABLE IF EXISTS #TEMP_LEGACY
                                                DROP TABLE IF EXISTS #TEMP_CONSTRAINTS
                                                DROP TABLE IF EXISTS #TEMP_LEGACY_CONSTRAINTS
                                                DROP TABLE IF EXISTS #TEMP_REFERENCED


                                                -- PEGANDO COLUNAS QUE POSSUEM CONSTRAINTS
	                                                SELECT DISTINCT A.CONSTRAINT_TYPE AS TYPE_CONSTRAINTS, B.COLUMN_NAME AS COLUMN_CONSTRAINTS, B.CONSTRAINT_NAME AS NAME_CONSTRAINT, A.TABLE_NAME AS NAME_TABLE
		                                                INTO #TEMP_CONSTRAINTS
		                                                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS A 
		                                                LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS B
		                                                ON	A.TABLE_NAME = B.TABLE_NAME and A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
		                                                WHERE A.TABLE_NAME = '{t.TableName}'

                                                -- PEGA A TABELA DA FK REFERENCIADA 
	                                                SELECT   
                                                   OBJECT_NAME(f.parent_object_id) AS table_name  
                                                   ,COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name  
                                                   ,OBJECT_NAME (f.referenced_object_id) AS referenced_object  
	                                                INTO #TEMP_REFERENCED
	                                                FROM sys.foreign_keys AS f  
	                                                INNER JOIN sys.foreign_key_columns AS fc   
	                                                 ON f.object_id = fc.constraint_object_id   
	                                                WHERE f.parent_object_id = OBJECT_ID('{t.TableName}');
	
                                                --PEGANDO VALORES AS COLUNAS COMUNS
                                                IF OBJECT_ID('#TEMP_LEGACY', N'U') IS NULL 
                                                    BEGIN 
                                                        SELECT COLUMN_NAME AS NAME, 
                                                        DATA_TYPE AS TYPE,
                                                        CHARACTER_MAXIMUM_LENGTH AS LENGTH,
		                                                NUMERIC_PRECISION AS PREC,
		                                                NUMERIC_SCALE AS SCALE,
                                                        IS_NULLABLE AS NULLABLE, 
                                                        COLLATION_NAME AS COLLATION,
                                                        (SELECT DISTINCT 1 FROM SYS.COLUMNS WHERE NAME = COLUMN_NAME AND IS_IDENTITY  > 0  AND object_id = Object_ID('{t.TableName}')) AS IS_IDENTITY 
		                                                INTO #TEMP_LEGACY 
                                                        FROM INFORMATION_SCHEMA.COLUMNS 
                                                        WHERE TABLE_NAME = '{t.TableName}' 
                                                END

                                                --PEGANDO VALORES DEFAULT
                                                select con.[name] NAME_DEFAULT,
                                                        t.[name]   TABLE_DEFAULT,
                                                        col.[name]  COLUMN_DEFAULT,
                                                        con.[definition]  VALOR_DEFAULT
		                                                INTO #TEMP_DEFAULT
			                                                from sys.default_constraints con
                                                        left outer join sys.objects t
                                                            on con.parent_object_id = t.object_id
                                                        left outer join sys.all_columns col
                                                            on con.parent_column_id = col.column_id
                                                            and con.parent_object_id = col.object_id
					                                                where t.[name] = '{t.TableName}'


                                                --UNINDO TODOS SELECTS
		                                                SELECT 
		                                                T1.NAME, 
		                                                T1.TYPE, 
		                                                T1.LENGTH, 
		                                                T1.PREC, 
		                                                T1.SCALE, 
		                                                T1.NULLABLE, 
		                                                T1.COLLATION, 
		                                                T1.IS_IDENTITY, 
		                                                T2.TYPE_CONSTRAINTS, 
		                                                T2.COLUMN_CONSTRAINTS, 
		                                                T2.NAME_CONSTRAINT,
		                                                T2.NAME_TABLE,
		                                                T3.NAME_DEFAULT,
		                                                T3.TABLE_DEFAULT,
		                                                T3.COLUMN_DEFAULT,
		                                                T3.VALOR_DEFAULT,
		                                                T4.table_name,
		                                                T4.constraint_column_name,
		                                                T4.referenced_object
		                                                INTO #TEMP_LEGACY_CONSTRAINTS 
		                                                from #TEMP_LEGACY T1
		                                                LEFT JOIN #TEMP_CONSTRAINTS T2 
			                                                ON T1.NAME = T2.COLUMN_CONSTRAINTS
		                                                LEFT JOIN #TEMP_DEFAULT T3 
			                                                ON T1.NAME = T3.COLUMN_DEFAULT
		                                                LEFT JOIN #TEMP_REFERENCED T4
		                                                ON T2.COLUMN_CONSTRAINTS = T4.constraint_column_name AND T2.NAME_TABLE = T4.table_name


                                                --CHAMANDO SELECT FINAL


                                                SELECT 
	                                                NAME, 
	                                                TYPE, 
	                                                LENGTH, 
	                                                PREC, 
	                                                SCALE, 
	                                                NULLABLE, 
	                                                COLLATION, 
	                                                IS_IDENTITY, 
	                                                TYPE_CONSTRAINTS, 
	                                                COLUMN_CONSTRAINTS, 
	                                                NAME_CONSTRAINT,
	                                                NAME_DEFAULT,
	                                                TABLE_DEFAULT,
	                                                COLUMN_DEFAULT,
	                                                VALOR_DEFAULT,
	                                                constraint_column_name,
	                                                referenced_object
                                                FROM #TEMP_LEGACY_CONSTRAINTS";

                    t.Columns = sqlCon.Query<Column>(columnTables).ToList();
                });
                #endregion

                AlterTables(tables);

                if (sqlCon.State == ConnectionState.Open)
                {
                    sqlCon.Close();
                }

                Console.WriteLine("Script gerado com sucesso, pressione qualquer tecla para ENCERRAR o programa");
                Console.ReadKey();

            }

            static void AlterTables(List<Tables> objects)
            {
                if (objects?.Count > 0)
                {
                    StringBuilder tables = new StringBuilder();

                    objects.ForEach(t =>
                    {
                        Tables DistinctT = new Tables();


                        tables.AppendLine($"IF OBJECT_ID('{t.TableName}', N'U') IS NULL");
                        tables.AppendLine("BEGIN");
                        tables.AppendLine($"    CREATE TABLE {t.TableName}");
                        tables.AppendLine("(");
                        int cont = 0;
                        var listPK = new List<String>();
                        var listPK1 = new List<String>();
                        var teste = new List<Column>();
                        var listConstraintRepet = new List<String>();
                        DistinctT.Columns = t.Columns.Distinct(new ColumnComparer()).ToList();
                        int total = DistinctT.Columns.Count;
                        int firstLoop = DistinctT.Columns.Count;
                        DistinctT.Columns.ForEach((c) =>
                        {

                            //var primary = t.Columns.Count(pk => pk.type_constraints == "PRIMARY KEY");
                            //if (primary > 1 && c.type_constraints == "PRIMARY KEY")
                            //{
                            //    listPK.Add(c.column_constraints); // Armazenando todas as PK em uma lista
                            //}

                            var chavesPK1 = DistinctT.Columns.Where(x => x.type_constraints == "PRIMARY KEY").ToList();
                            var primary1 = chavesPK1.Count();
                            if (total == firstLoop)
                            {
                                var addListPK1 = from listpk1 in chavesPK1 select listpk1;
                                foreach (var listpk1 in addListPK1)
                                {
                                    listPK1.Add(listpk1.column_constraints);
                                }
                            }

                            var filtroConstraint = DistinctT.Columns.Where(x => listPK1.Any(pks => pks == x.column_constraints) && x.type_constraints == "FOREIGN KEY" || x.type_constraints == "UNIQUE").ToList();
                            var constraintRepetida = filtroConstraint.Count();
                            var listConstraintRep = from constraintRepet in filtroConstraint select constraintRepet;

                            foreach (var constraintRepet in listConstraintRep)
                            {
                                teste.Add(constraintRepet);
                                //DistinctT.Columns.Remove(constraintRepet);
                                listConstraintRepet.Add(constraintRepet.type_constraints);
                            }

                            if (constraintRepetida > 0 && total == firstLoop)
                            {
                                total -= constraintRepetida;
                            }

                            if (constraintRepetida <= 0 || filtroConstraint.Any(x => x.type_constraints == c.type_constraints || x.name_constraint == c.name_constraint)) // PROVAVELMENTE A LOGICA ESTA ERRADA, DEVE SER NECESSARIO VERIFICAR SE A FK É SÓ FK E NAO PK FK
                            {
                                Console.WriteLine(c.name, c.type_constraints + "AQUI");

                            }



                            if (constraintRepetida <= 0 || filtroConstraint.Any(x => x.type_constraints == c.type_constraints || x.name_constraint == c.name_constraint)) // PROVAVELMENTE A LOGICA ESTA ERRADA, DEVE SER NECESSARIO VERIFICAR SE A FK É SÓ FK E NAO PK FK
                            {

                                if (c.length == "-1")// Na consulta, quando o Length é "max" ele retorna -1
                                    c.length = "max";// Então forcei ele retornar "max"

                                var types = new List<string> { "int", "money", "bigint", "smallmoney", "tinyint", "smallint" };

                                if (c.type_constraints == "PRIMARY KEY" && c.is_identity == 1 && primary1 == 1)
                                    tables.Append($" {c.name} {c.type} IDENTITY(1,1) PRIMARY KEY");

                                else if (c.type_constraints == "PRIMARY KEY" && c.is_identity == 0 && primary1 == 1)
                                    tables.Append($" {c.name} {c.type} PRIMARY KEY");

                                else if (c.length == null && c.is_identity == 1 && c.column_constraints != c.name)
                                    tables.Append($" {c.name} {c.type} IDENTITY(1,1)");

                                else if (c.length == null && c.is_identity == 1 && c.column_constraints != c.name && c.type_constraints == "UNIQUE")
                                    tables.Append($" {c.name} {c.type} IDENTITY(1,1) UNIQUE");

                                else if (c.length == null && types.Any(x => x == c.type) && c.type_constraints != "UNIQUE")
                                    tables.Append($" {c.name} {c.type}"); //Exemplo: int (somente)

                                else if (c.length == null && types.Any(x => x == c.type) && c.type_constraints == "UNIQUE")
                                    tables.Append($" {c.name} {c.type} UNIQUE"); //Exemplo: int  UNIQUE

                                else if (c.prec != null)
                                    tables.Append($" {c.name} {c.type}({c.prec},{c.scale}) "); //Exemplo: numeric(8,3)

                                else if (c.prec != null && c.type_constraints == "UNIQUE")
                                    tables.Append($" {c.name} {c.type}({c.prec},{c.scale}) UNIQUE"); //Exemplo: numeric(8,3)

                                else if (c.length == null)
                                    tables.Append($" {c.name} {c.type}"); //Exemplo: varchar ou int tem parenteses()

                                else if (c.length != null && c.type_constraints == "UNIQUE")
                                    tables.Append($" {c.name} {c.type}({c.length}) UNIQUE"); //Exemplo: varchar(25) or int (null) UNIQUE

                                else if (c.type == "image" || c.type == "text")
                                    tables.Append($" {c.name} {c.type}"); //Exemplo: image

                                else
                                    tables.Append($" {c.name} {c.type}({c.length})"); //Exemplo: varchar(25) or int (null)

                                if (cont == (total - 1))
                                {
                                    if (c.nullable == "NO")
                                    {
                                        tables.AppendLine($" NOT NULL");
                                        if (listPK1.Any(x => x == x) && primary1 > 1)  // PENSAR NA LOGICA AQUI
                                        {
                                            tables.AppendLine($"CONSTRAINT PK_{t.TableName} PRIMARY KEY ({string.Join(",", listPK1)})");
                                        }
                                        tables.AppendLine(")");
                                    }
                                    else
                                    {
                                        tables.AppendLine($" NULL");
                                        if (listPK1.Any(x => x == x) && primary1 > 1)  // PENSAR NA LOGICA AQUI
                                        {
                                            tables.AppendLine($"CONSTRAINT PK_{t.TableName} PRIMARY KEY ({string.Join(",", listPK1)})");
                                        }
                                        tables.AppendLine(")");
                                    }
                                }
                                else
                                {
                                    if (c.nullable == "NO")
                                        tables.AppendLine($" NOT NULL,");
                                    else
                                        tables.AppendLine($" NULL,");
                                }
                                cont++;
                                firstLoop -= total;
                            }

                        });
                        DistinctT.Columns.ForEach((c) =>
                        {
                            if (c.type_constraints == "FOREIGN KEY")
                                tables.AppendLine($" ALTER TABLE {t.TableName} ADD CONSTRAINT {c.name_constraint} FOREIGN KEY ({c.column_constraints}) REFERENCES {c.referenced_object} ({c.column_constraints});");

                            if (c.column_default != null)
                                tables.AppendLine($" ALTER TABLE {t.TableName} ADD CONSTRAINT {c.name_default} DEFAULT {c.valor_default} FOR {c.column_default};");


                        });
                        //  tables.AppendLine(")");
                        tables.AppendLine("END");
                        tables.AppendLine("GO");
                        tables.AppendLine();
                        //tables.AppendLine("-----------------------------------------------------------------------------------------------------------------");
                        tables.AppendLine();
                        // VERIFICAR AS COLUNAS
                        DistinctT.Columns.ForEach((c) =>
                        {
                            var primary = t.Columns.Count(pk => pk.type_constraints == "PRIMARY KEY");
                            if (primary > 1 && c.type_constraints == "PRIMARY KEY")
                            {
                                listPK.Add(c.column_constraints); // Armazenando todas as PK em uma lista
                            }

                            var chavesPK1 = t.Columns.Where(x => x.type_constraints == "PRIMARY KEY").ToList();
                            //var primary1 = chavesPK1.Count();
                            if (total == firstLoop)
                            {
                                var addListPK1 = from listpk1 in chavesPK1 select listpk1;
                                //ADICIONAR UM FILTRO AQUI PARA NAO FICAR ARMAZENANDO DIVERSOS ITENS NA LIST

                                foreach (var listpk1 in addListPK1)
                                {
                                    listPK1.Add(listpk1.column_constraints); // Armazenando uma PK por vez em cada loop (tentar usar distinct dps)
                                }
                            }

                            var filtroConstraint = t.Columns.Where(x => listPK1.Any(pks => pks == x.column_constraints) && x.type_constraints == "FOREIGN KEY" || x.type_constraints == "UNIQUE").ToList();
                            var constraintRepetida = filtroConstraint.Count();
                            var listConstraintRep = from constraintRepet in filtroConstraint select constraintRepet;

                            foreach (var constraintRepet in listConstraintRep)
                            {
                                listConstraintRepet.Add(constraintRepet.type_constraints);
                            }

                            if (constraintRepetida > 0 && total == firstLoop)
                            {
                                total -= constraintRepetida;
                            }

                            if (constraintRepetida <= 0 || listConstraintRepet.Any(x => x != c.type_constraints))
                            {
                                if (c.length == "-1")// Na consulta, quando o Length é "max" ele retorna -1
                                    c.length = "max";// Então forcei ele retornar "max"

                                var types = new List<string> { "int", "money", "bigint", "smallmoney", "tinyint", "smallint" };
                                tables.Append($" if not exists(Select * From sys.columns Where object_id = Object_ID('{t.TableName}') and name = '{c.name}')");
                                tables.AppendLine();
                                tables.AppendLine($"BEGIN");
                                tables.Append($"ALTER TABLE {t.TableName}");
                                //Segunda condição verifica se o tamanho do type é nulo, se for (PROVAVEL INT)
                                if (c.type_constraints == "PRIMARY KEY" && c.is_identity == 1 && primary == 1)
                                    tables.Append($" ADD {c.name} {c.type} IDENTITY(1,1) PRIMARY KEY");

                                else if (c.type_constraints == "PRIMARY KEY" && c.is_identity == 0 && primary == 1)
                                    tables.Append($" ADD {c.name} {c.type} PRIMARY KEY");

                                else if (c.length == null && c.is_identity == 1 && c.column_constraints != c.name)
                                    tables.Append($" ADD {c.name} {c.type} IDENTITY(1,1)");

                                else if (c.length == null && c.is_identity == 1 && c.column_constraints != c.name && c.type_constraints == "UNIQUE")
                                    tables.Append($" ADD {c.name} {c.type} IDENTITY(1,1) UNIQUE");

                                else if (c.length == null && types.Any(x => x == c.type) && c.type_constraints != "UNIQUE")
                                    tables.Append($" ADD {c.name} {c.type}"); //Exemplo: int (somente)

                                else if (c.length == null && types.Any(x => x == c.type) && c.type_constraints == "UNIQUE")
                                    tables.Append($" ADD {c.name} {c.type} UNIQUE"); //Exemplo: int  UNIQUE

                                else if (c.prec != null)
                                    tables.Append($" ADD {c.name} {c.type}({c.prec},{c.scale}) "); //Exemplo: numeric(8,3)

                                else if (c.prec != null && c.type_constraints == "UNIQUE")
                                    tables.Append($" ADD {c.name} {c.type}({c.prec},{c.scale}) UNIQUE"); //Exemplo: numeric(8,3)

                                else if (c.length == null)
                                    tables.Append($" ADD {c.name} {c.type}"); //Exemplo: varchar ou int tem parenteses()

                                else if (c.length != null && c.type_constraints == "UNIQUE")
                                    tables.Append($" ADD {c.name} {c.type}({c.length}) UNIQUE"); //Exemplo: varchar(25) or int (null) UNIQUE

                                else if (c.type == "image" || c.type == "text")
                                    tables.Append($" ADD {c.name} {c.type}"); //Exemplo: image

                                else
                                    tables.Append($" ADD {c.name} {c.type}({c.length})"); //Exemplo: varchar(25) or int (null)
                                tables.AppendLine($" END");
                            }

                        });
                        tables.AppendLine($"GO");
                        tables.AppendLine("----------------------------------------------------------------------------------------------------------------");
                        tables.AppendLine();
                    });

                    string script = tables.ToString();

                    string timestamp = JavascriptGetTime();

                    CreateTextFile(tablesPath, $"Mig_{timestamp}_UPDATE_THE_TABLES.sql", script);

                }
            }
        }

        private static void DeleteFile(string file)
        {
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }

        //Metodo de criação chama os metodos de verificação tambem
        private static void CreateDirectory(string path)
        {
            ClearDirectory(path);

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

        }

        private static void ClearDirectory(string path)
        {
            if (Directory.Exists(path))
            {
                var files = Directory.GetFiles(path);

                for (int i = 0; i < files.Length; i++)
                {
                    File.Delete(files[i]);
                }
            }

        }
        private static void CreateTextFile(string path, string filename, string text)
        {
            string filePath = Path.Combine(path, filename);

            if (!File.Exists(filePath))
            {
                File.WriteAllText(filePath, text);
            }
        }
        private static string JavascriptGetTime()
        {
            return ((long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds).ToString();
        }
    }
}