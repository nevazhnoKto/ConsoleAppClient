using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using RabbitMQ.Client;
using System.Threading;

namespace ConsoleAppClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread threadSender = new Thread(new ThreadStart(SenderThread));
            threadSender.Start();
            Thread threadUpdateRabbit = new Thread(new ThreadStart(UpdateRabbit));
            threadUpdateRabbit.Start();
            Console.WriteLine("Для выхода нажмите Esc");
            while (Console.ReadKey().Key != ConsoleKey.Escape)
            {
                Console.WriteLine("Для выхода нажмите Esc");
            }
            threadUpdateRabbit.Abort();
            threadSender.Abort();
        }

        // Поток, срабатывающий раз в 10 сек.
        // Проверяет наличие не отправленных сообщений из базы в брокер и отправляет их.        
        public static void UpdateRabbit()
        {
            ClassForRabbit rabbit = new ClassForRabbit();
            ClassForDataBase db = new ClassForDataBase();
            List<Int32> inds;
            List<String> messages;
            while (true)
            {
                if (rabbit.GetConnection())
                {
                    Thread.Sleep(10000);
                    Int32 ind = 0;
                    db.GetUnSendingMEssage(out messages, out inds);
                    Console.WriteLine("Зашел во внешний цикл, колиество неотправленных = {0}", messages.Count().ToString());
                    foreach (String messaage in messages)
                    {
                        // если сообщение отправлено, то добавить измененние в локальной базе  
                        if (rabbit.DoSend(messaage))
                            db.ChangeSendFlagAsync(inds[ind], 1);
                        ind++;
                    }
                }

            }
        }

        // Главный поток для формировния и отправки сообщений.
        // Сообщения отправляются в локальную базу и в брокер сообщений.
        public static void SenderThread()
        {
            ClassForRabbit rabbit = new ClassForRabbit();
            ClassForDataBase db = new ClassForDataBase();
            // Получить индекс последнего добавленного в локальную бд.
            int lastInd = db.GetLastIndexInDb();
            while (true)
            {
                lastInd++;
                string timeNow = DateTime.Now.ToShortTimeString();
                string randomText = "randomText";
                string hash = "hash";
                string message = String.Join("_", lastInd.ToString(), timeNow, randomText, hash);
                db.AddStringAnsync(timeNow, randomText, hash);
                Console.WriteLine("Добавлено в базу - {0}", message);
                //Отправить в брокер, если включен.
                if (rabbit.DoSend(message))
                {
                    // Добавить в базу признак, что сообщение отправлено в брокер.
                    db.ChangeSendFlagAsync(lastInd, 1);
                }
                Thread.Sleep(2000);
            }
        }
        //Класс для работы с брокером сообщений RabbitMQ.
        class ClassForRabbit
        {
            //Проверка соединения с брокером.
            public bool GetConnection()
            {
                var factory = new ConnectionFactory()
                {
                    HostName = "localhost"
                };
                try
                {
                    using (var connection = factory.CreateConnection())
                    {
                        using (var channel = connection.CreateModel())
                        {
                            // Возвращаем состояние канала.                  
                            return channel.IsOpen;
                        }
                    }
                }
                catch
                {
                    return false;
                }

            }
            //Отправка сообщения в брокер.
            public bool DoSend(String message)
            {
                var factory = new ConnectionFactory()
                {
                    HostName = "localhost"
                };
                try
                {
                    using (var connection = factory.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        // Если канал закрыт, выходим с результатом false.
                        if (channel.IsClosed)
                            return false;
                        channel.QueueDeclare(queue: "example1",
                                             durable: true,
                                             exclusive: false,
                                             autoDelete: false,
                                             arguments: null);
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: "",
                                             routingKey: "example1",
                                             basicProperties: null,
                                             body: body);
                        Console.WriteLine("Отправлено сообщение - {0}", message);
                        return true;
                    }
                }
                catch
                {
                    Console.WriteLine("Подключение к RabbitMQ не установленно");
                    return false;
                }

            }
        }
    }

    // Класс для работы с локальной базой данных.
    class ClassForDataBase
    {
        // Метод для поиска не отправленных сообщений из БД в брокер.
        // Принимает переменную  для сообщений и переменную для индексов.
        // returns Список сообщений и соответствующий список индексов.
        public void GetUnSendingMEssage(out List<String> mess, out List<Int32> inds)
        {
            inds = new List<int>();
            mess = new List<String>();
            String connectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=exampledb;Integrated Security=True";
            String sqlExpression = String.Format("SELECT * FROM ClientDB WHERE IsSending=0");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(sqlExpression, connection);
                SqlDataReader reader = command.ExecuteReader();
                if (reader.HasRows) // если есть данные
                {
                    while (reader.Read()) // построчно считываем данные
                    {
                        // Запись в список всех данных, не отправленых на сервер
                        Int32 id = reader.GetInt32(0);
                        string date = reader.GetString(1);
                        string message = reader.GetString(2);
                        string hash = reader.GetString(1);
                        mess.Add(String.Join("_", id.ToString(), date, message, hash));
                        inds.Add(id);
                    }
                }
                reader.Close();
            }
        }

        // Метод, позволяющий найти индекс последнего занесенного сообщения в БД.        
        // returns Индекс последнего сообщения.
        public int GetLastIndexInDb()
        {
            Int32 lastInd = -1;
            string connectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=exampledb;Integrated Security=True";
            string sqlExpression = String.Format("SELECT MAX(ID) FROM ClientDB");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(sqlExpression, connection);
                SqlDataReader reader = command.ExecuteReader();
                if (reader.HasRows) // если есть данные
                {
                    if (reader.Read())
                    {
                        lastInd = reader.GetInt32(0);
                    }
                }
                reader.Close();
            }
            return lastInd;
        }
        // Метод записи сообщения в бд. 
        // Принимает время, текс, хэш.
        public async void AddStringAnsync(string timeNow, string message, string hash)
        {
            string connectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=exampledb;Integrated Security=True";
            string sqlExpression = String.Format("INSERT INTO ClientDB (Date, Message, Hash, IsSending) VALUES ('{0}','{1}','{2}', 0)", timeNow, message, hash);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                SqlCommand command = new SqlCommand(sqlExpression, connection);
                await command.ExecuteNonQueryAsync(); // асинхронная запись строки в бд
            }
        }

        // Метод для изменения флага IsSendin(флаг отправки в брокер)
        // Принимает индекс сообщения и значение, которое нужно установить.
        public async void ChangeSendFlagAsync(int ind, int newValue)
        {
            string connectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=exampledb;Integrated Security=True";
            string sqlExpression = String.Format("UPDATE ClientDB SET IsSending={1} WHERE Id='{0}'", ind, newValue);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                SqlCommand command = new SqlCommand(sqlExpression, connection);
                await command.ExecuteNonQueryAsync(); // асинхронная запись строки в бд
            }
        }
    }


}
