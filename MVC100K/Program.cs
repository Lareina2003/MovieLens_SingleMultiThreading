using MovieLens.Controllers;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace MovieLens
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("🎬 MovieLens OLAP - MVC (Single File per Layer)");

            Console.Write("Enter path to MovieLens 100k folder: ");
            string path = Console.ReadLine().Trim('"');

            if (!Directory.Exists(path))
            {
                Console.WriteLine("❌ Folder not found!");
                return;
            }

            var users = DataController.LoadUsers(path);
            var movies = DataController.LoadMovies(path);
            var ratings = DataController.LoadRatings(path);

            // SINGLE THREAD
            Console.WriteLine("\n🧵 Running SINGLE-THREAD reports...");
            var sw1 = Stopwatch.StartNew();
            ReportController.GenerateReports(users, movies, ratings, Path.Combine("Reports", "Single"));
            sw1.Stop();
            Console.WriteLine($"✅ Single-thread done in {sw1.Elapsed.TotalSeconds:F2} sec");

            // MULTI THREAD
            Console.WriteLine("\n⚡ Running MULTI-THREAD reports...");
            var sw2 = Stopwatch.StartNew();
            Parallel.Invoke(() => ReportController.GenerateReports(users, movies, ratings, Path.Combine("Reports", "Multi")));
            sw2.Stop();
            Console.WriteLine($"✅ Multi-thread done in {sw2.Elapsed.TotalSeconds:F2} sec");

            Console.WriteLine("\n🎉 All reports completed successfully!");
        }
    }
}
