using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

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
            const string connectionString = "Data Source=CLK-NOTE_24\\SQLEXPRESS; Initial Catalog=AdventureWorks2019; Integrated Security=SSPI;";
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

                 tables.ForEach(t =>
                {
                    string columnTables = $@"SELECT COLUMN_NAME as name,
                                            DATA_TYPE as type, 
                                            CHARACTER_MAXIMUM_LENGTH as length, 
                                            IS_NULLABLE as nullable,
                                            (SELECT 1 FROM SYS.COLUMNS WHERE NAME = COLUMN_NAME AND IS_IDENTITY = 1) as is_identity 
                                            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{t.TableName}';";


                    t.Columns = sqlCon.Query<Column>(columnTables).ToList();
                });

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

                        tables.AppendLine($"IF OBJECT_ID('{t.TableName}', N'U') IS NULL");
                        tables.AppendLine("BEGIN");
                        tables.AppendLine($"    CREATE TABLE {t.TableName}");
                        tables.AppendLine("(");
                        int cont = 0;
                        int total = t.Columns.Count;
                        t.Columns.ForEach((c) =>
                        {
                            //Primeira condição percorre o Length para tirar as virgulas de todas tabelas
                            if (cont == (total - 1))
                            {

                                //Segunda condição verifica se o tamanho do type é nulo, se for (PROVAVEL INT)
                                if (c.length == null && c.is_identity == 1)
                                    tables.Append($" {c.name} {c.type} IDENTITY(1,1) ");
                                else if (c.length == null)
                                    tables.Append($" {c.name} {c.type}");
                                else
                                    tables.Append($" {c.name} {c.type}({c.length}) "); //Example: varchar(25) or int (null)
                                if (c.nullable == "NO")
                                    tables.AppendLine($" not null ");
                                else
                                    tables.AppendLine($" null");

                            }
                            //else coloca virgula
                            else
                            {
                                if (c.length == null && c.is_identity == 1)
                                    tables.Append($" {c.name} {c.type} IDENTITY(1,1) ");
                                else if (c.length == null)
                                    tables.Append($" {c.name} {c.type}");
                                else
                                    tables.Append($" {c.name} {c.type}({c.length})"); //Example: varchar(25) or int (null)

                                if (c.nullable == "NO")
                                    tables.AppendLine($" not null,");
                                else
                                    tables.AppendLine($" null,");

                            }
                            cont++;
                        });
                        tables.AppendLine(")");
                        tables.AppendLine("END");
                        tables.AppendLine("GO");
                        tables.AppendLine();
                        //tables.AppendLine("-----------------------------------------------------------------------------------------------------------------");
                        tables.AppendLine();
                        // VERIFICAR AS COLUNAS
                        t.Columns.ForEach((c) =>
                        {
                            tables.Append($" if not exists(Select * From sys.columns Where object_id = Object_ID('{t.TableName}') and name = '{c.name}')");
                            tables.AppendLine();
                            tables.AppendLine($"BEGIN");
                            tables.Append($"ALTER TABLE {t.TableName}");
                            //Segunda condição verifica se o tamanho do type é nulo, se for (PROVAVEL INT)
                            if (c.length == null && c.is_identity == 1)
                                tables.Append($" {c.name} {c.type} IDENTITY(1,1)");
                            else if (c.length == null)
                                tables.Append($" {c.name} {c.type}");
                            else
                                tables.Append($" {c.name} {c.type}({c.length})"); //Example: varchar(25) or int (null)
                            if (c.nullable == "NO")
                                tables.AppendLine($" not null");
                            else
                                tables.AppendLine($" null");
                            tables.AppendLine($" END");
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