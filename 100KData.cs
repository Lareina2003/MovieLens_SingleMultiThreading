

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace MovieLensOLAP
{
    class Program
    {
        // Genre order used in the original u.item file (19 genres)
        static readonly string[] GenresOrder = new[] {
            "unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"
        };

        static readonly string[] TargetGenres = new[] { "Action", "Drama", "Comedy", "Fantasy" };

        static void Main(string[] args)
        {
            Console.WriteLine("MovieLens OLAP - C# Multi-threaded sample (Visual Studio 2022)");

            Console.Write("Enter path to MovieLens 100k folder (containing u.data, u.item, u.user): ");
            string dataDir = Console.ReadLine().Trim('"');

            int chunkSize = 10000;
            int minRatings = 20;

            if (!Directory.Exists(dataDir))
            {
                Console.WriteLine("Data folder not found: " + dataDir);
                return;
            }

            // --- SINGLE THREAD ---
            Console.WriteLine("\n🧵 Running SINGLE THREAD reports...");
            currentReportFolder = Path.Combine("Reports", "Single");
            var sw1 = Stopwatch.StartNew();
            Run(dataDir, chunkSize, minRatings, useThreads: false);
            sw1.Stop();
            Console.WriteLine($"✅ Single-thread reports done in {sw1.Elapsed.TotalSeconds:F2} sec");

            // --- MULTI THREAD ---
            Console.WriteLine("\n⚡ Running MULTI THREAD reports...");
            currentReportFolder = Path.Combine("Reports", "Multi");
            var sw2 = Stopwatch.StartNew();
            Run(dataDir, chunkSize, minRatings, useThreads: true);
            sw2.Stop();
            Console.WriteLine($"✅ Multi-thread reports done in {sw2.Elapsed.TotalSeconds:F2} sec");

            Console.WriteLine("\n🎉 All reports completed!");
        }


        static void Run(string dataDir, int chunkSize, int minRatings, bool useThreads)
        {
            var swTotal = Stopwatch.StartNew();

            Console.WriteLine($"Loading data from: {dataDir}");
            var users = LoadUsers(Path.Combine(dataDir, "u.user"));
            var movies = LoadMovies(Path.Combine(dataDir, "u.item"));
            var ratings = LoadRatings(Path.Combine(dataDir, "u.data"));

            Console.WriteLine($"Loaded {users.Count} users, {movies.Count} movies, {ratings.Count} ratings");

            int threadCount = (ratings.Count + chunkSize - 1) / chunkSize; // ceil
            Console.WriteLine($"Chunk size: {chunkSize}. Number of threads (records/chunk): {threadCount}");

            var agg = ProcessRatings(ratings, users, movies, chunkSize, useThreads);

            Console.WriteLine("Merging & generating reports...");

            Directory.CreateDirectory("Reports");

            // 1) Top 10 Movies (General)
            WriteTopN("Top10_General", GetTopN(agg.MovieTotals, movies, 10, minRatings));

            // 2) Top 10 by gender
            WriteTopN("Top10_Male", GetTopN(agg.MaleTotals, movies, 10, minRatings));
            WriteTopN("Top10_Female", GetTopN(agg.FemaleTotals, movies, 10, minRatings));

            // 3) Top 10 for target genres
            foreach (var g in TargetGenres)
            {
                if (agg.GenreTotals.ContainsKey(g))
                    WriteTopN($"Top10_Genre_{Sanitize(g)}", GetTopN(agg.GenreTotals[g], movies, 10, minRatings));
            }

            // 4) Age-wise
            WriteTopN("Top10_Age_Under18", GetTopN(agg.AgeGroupTotals[AgeBucket.Under18], movies, 10, minRatings));
            WriteTopN("Top10_Age_18to29", GetTopN(agg.AgeGroupTotals[AgeBucket.Age18To29], movies, 10, minRatings));
            WriteTopN("Top10_Age_30plus", GetTopN(agg.AgeGroupTotals[AgeBucket.Age30Plus], movies, 10, minRatings));

            swTotal.Stop();
            Console.WriteLine($"All reports completed in {swTotal.Elapsed.TotalSeconds:F2} sec. Reports folder: {Path.GetFullPath("Reports")}");
        }

        // ---------- Parsing helpers ----------
        static Dictionary<int, User> LoadUsers(string path)
        {
            var dict = new Dictionary<int, User>();
            foreach (var line in File.ReadLines(path))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                // original file uses '|' as separator: user id | age | gender | occupation | zip
                var parts = line.Split('|');
                if (parts.Length < 3) continue;
                if (!int.TryParse(parts[0], out int uid)) continue;
                int age = 0; int.TryParse(parts[1], out age);
                string gender = parts[2];
                dict[uid] = new User { Id = uid, Age = age, Gender = gender };
            }
            return dict;
        }

        static Dictionary<int, Movie> LoadMovies(string path)
        {
            var dict = new Dictionary<int, Movie>();
            foreach (var line in File.ReadLines(path))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var parts = line.Split('|');
                // u.item format: movie id | movie title | release date | video release date | IMDb URL | [19 genre flags]
                if (parts.Length < 6) continue;
                if (!int.TryParse(parts[0], out int mid)) continue;
                string title = parts[1];
                var genres = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // genre flags start at index 5
                for (int gi = 0; gi < GenresOrder.Length; gi++)
                {
                    int idx = 5 + gi;
                    if (idx < parts.Length && parts[idx] == "1")
                    {
                        genres.Add(GenresOrder[gi]);
                    }
                }
                dict[mid] = new Movie { Id = mid, Title = title, Genres = genres };
            }
            return dict;
        }

        static List<Rating> LoadRatings(string path)
        {
            var list = new List<Rating>();
            foreach (var line in File.ReadLines(path))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                // u.data is tab separated: user id \t item id \t rating \t timestamp
                var parts = line.Split('\t');
                if (parts.Length < 3) continue;
                if (!int.TryParse(parts[0], out int uid)) continue;
                if (!int.TryParse(parts[1], out int mid)) continue;
                if (!int.TryParse(parts[2], out int score)) continue;
                long ts = 0; long.TryParse(parts.Length > 3 ? parts[3] : "0", out ts);
                list.Add(new Rating { UserId = uid, MovieId = mid, Score = score, Timestamp = ts });
            }
            return list;
        }

        // ---------- Aggregation & multi-thread processing ----------
        enum AgeBucket { Under18 = 0, Age18To29 = 1, Age30Plus = 2 }

        class Aggregation
        {
            public Dictionary<int, (double Sum, int Count)> MovieTotals = new();
        public Dictionary<int, (double Sum, int Count)> MaleTotals = new();
        public Dictionary<int, (double Sum, int Count)> FemaleTotals = new();
        public Dictionary<string, Dictionary<int, (double Sum, int Count)>> GenreTotals = new();
        public Dictionary<AgeBucket, Dictionary<int, (double Sum, int Count)>> AgeGroupTotals = new();

        public Aggregation()
        {
            foreach (var g in TargetGenres) GenreTotals[g] = new Dictionary<int, (double, int)>();
            AgeGroupTotals[AgeBucket.Under18] = new Dictionary<int, (double, int)>();
            AgeGroupTotals[AgeBucket.Age18To29] = new Dictionary<int, (double, int)>();
            AgeGroupTotals[AgeBucket.Age30Plus] = new Dictionary<int, (double, int)>();
        }
        }

        static Aggregation ProcessRatings(List<Rating> ratings, Dictionary<int, User> users, Dictionary<int, Movie> movies, int chunkSize, bool useThreads)
        {
            int total = ratings.Count;
            int threadCount = (total + chunkSize - 1) / chunkSize;

            if (!useThreads || threadCount == 1)
            {
                // Single-threaded processing
                Console.WriteLine("Processing sequentially (no threads)...");
                return ProcessChunk(ratings, 0, total, users, movies);
            }

            Console.WriteLine($"Processing with {threadCount} threads...");
            var results = new Aggregation[threadCount];
            var threads = new Thread[threadCount];

            for (int i = 0; i < threadCount; i++)
            {
                int idx = i;
                int start = idx * chunkSize;
                int end = Math.Min(start + chunkSize, total);

                threads[idx] = new Thread(() =>
                {
                    // Mark as foreground thread (IsBackground = false) so app waits until completion
                    Thread.CurrentThread.IsBackground = false;
                    results[idx] = ProcessChunk(ratings, start, end, users, movies);
                    Console.WriteLine($"Thread {idx + 1}/{threadCount} finished: processed [{start}..{end - 1}]");
                });

                // Start thread
                threads[idx].Start();
            }

            // Wait for threads
            for (int i = 0; i < threadCount; i++) threads[i].Join();

            // Merge per-thread results
            var merged = new Aggregation();
            foreach (var r in results)
            {
                MergeDict(merged.MovieTotals, r.MovieTotals);
                MergeDict(merged.MaleTotals, r.MaleTotals);
                MergeDict(merged.FemaleTotals, r.FemaleTotals);
                foreach (var g in TargetGenres) MergeDict(merged.GenreTotals[g], r.GenreTotals[g]);
                MergeDict(merged.AgeGroupTotals[AgeBucket.Under18], r.AgeGroupTotals[AgeBucket.Under18]);
                MergeDict(merged.AgeGroupTotals[AgeBucket.Age18To29], r.AgeGroupTotals[AgeBucket.Age18To29]);
                MergeDict(merged.AgeGroupTotals[AgeBucket.Age30Plus], r.AgeGroupTotals[AgeBucket.Age30Plus]);
            }

            return merged;
        }

        static Aggregation ProcessChunk(List<Rating> ratings, int start, int end, Dictionary<int, User> users, Dictionary<int, Movie> movies)
        {
            var agg = new Aggregation();

            for (int i = start; i < end; i++)
            {
                var r = ratings[i];
                if (!users.TryGetValue(r.UserId, out var u)) continue;
                if (!movies.TryGetValue(r.MovieId, out var m)) continue;

                // Movie totals
                Add(agg.MovieTotals, r.MovieId, r.Score);

                // Gender
                if (string.Equals(u.Gender, "M", StringComparison.OrdinalIgnoreCase)) Add(agg.MaleTotals, r.MovieId, r.Score);
                else if (string.Equals(u.Gender, "F", StringComparison.OrdinalIgnoreCase)) Add(agg.FemaleTotals, r.MovieId, r.Score);

                // Genres - only target genres
                foreach (var g in TargetGenres)
                {
                    if (m.Genres.Contains(g)) Add(agg.GenreTotals[g], r.MovieId, r.Score);
                }

                // Age groups (configurable buckets)
                var bucket = GetAgeBucket(u.Age);
                Add(agg.AgeGroupTotals[bucket], r.MovieId, r.Score);
            }

            return agg;
        }

        static AgeBucket GetAgeBucket(int age)
        {
            // Chosen boundaries:
            // Under18: age < 18
            // 18to29: 18 <= age < 30
            // 30plus: age >= 30
            if (age < 18) return AgeBucket.Under18;
            if (age < 30) return AgeBucket.Age18To29;
            return AgeBucket.Age30Plus;
        }

        static void Add(Dictionary<int, (double Sum, int Count)> dict, int movieId, int score)
        {
            if (dict.TryGetValue(movieId, out var t)) dict[movieId] = (t.Sum + score, t.Count + 1);
            else dict[movieId] = (score, 1);
        }

        static void MergeDict(Dictionary<int, (double Sum, int Count)> target, Dictionary<int, (double Sum, int Count)> src)
        {
            foreach (var kv in src)
            {
                if (target.TryGetValue(kv.Key, out var t)) target[kv.Key] = (t.Sum + kv.Value.Sum, t.Count + kv.Value.Count);
                else target[kv.Key] = kv.Value;
            }
        }

        // ---------- Reporting ----------
        class ReportRow
    {
        public int MovieId; public string Title; public double Avg; public int Count; }

        static List<ReportRow> GetTopN(Dictionary<int, (double Sum, int Count)> source, Dictionary<int, Movie> movies, int n, int minRatings)
        {
            var q = source
                .Where(kv => kv.Value.Count >= minRatings)
                .Select(kv => new ReportRow { MovieId = kv.Key, Title = movies.TryGetValue(kv.Key, out var m) ? m.Title : "<unknown>", Avg = kv.Value.Sum / kv.Value.Count, Count = kv.Value.Count })
                .OrderByDescending(r => r.Avg)
                .ThenByDescending(r => r.Count)
                .ThenBy(r => r.Title)
                .Take(n)
                .ToList();
            return q;
        }
        static string currentReportFolder = "Reports";

        static void WriteTopN(string reportName, List<ReportRow> rows)
        {
            Directory.CreateDirectory(currentReportFolder);
            var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var filename = Path.Combine(currentReportFolder, $"report_{Sanitize(reportName)}_{ts}.csv");
            var sb = new StringBuilder();
            sb.AppendLine("Rank,MovieId,Title,AverageRating,RatingCount");
            int rank = 1;
            foreach (var r in rows)
            {
                sb.AppendLine($"{rank},{r.MovieId},\"{EscapeCsv(r.Title)}\",{r.Avg:F3},{r.Count}");
                rank++;
            }
            File.WriteAllText(filename, sb.ToString(), Encoding.UTF8);
            Console.WriteLine($"Saved {rows.Count} rows -> {filename}");
        }


        static string EscapeCsv(string s) => s?.Replace("\"", "\"\"") ?? "";
        static string Sanitize(string s) => new string(s.Where(c => !Path.GetInvalidFileNameChars().Contains(c)).ToArray()).Replace(' ', '_');

        // ---------- Data models ----------
        class User { public int Id; public int Age; public string Gender; }
    class Movie { public int Id; public string Title; public HashSet<string> Genres; }
    class Rating { public int UserId; public int MovieId; public int Score; public long Timestamp; }
}
}
