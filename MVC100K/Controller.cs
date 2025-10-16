using MovieLens.Models;
using MovieLens.Views;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MovieLens.Controllers
{
    public static class DataController
    {
        public static List<User> LoadUsers(string path)
        {
            string file = Path.Combine(path, "u.user");
            return File.ReadLines(file).Select(line =>
            {
                var p = line.Split('|');
                return new User
                {
                    UserId = int.Parse(p[0]),
                    Age = int.Parse(p[1]),
                    Gender = p[2],
                    Occupation = p[3]
                };
            }).ToList();
        }
        public static List<Movie> LoadMovies(string path)
        {
            string file = Path.Combine(path, "u.item");

            return File.ReadLines(file).Select(line =>
            {
                var p = line.Split('|');

                // Some lines might be incomplete — handle safely
                if (p.Length < 6)
                    return null;

                var movie = new Movie
                {
                    MovieId = int.Parse(p[0]),
                    Title = p[1],
                    Genres = new List<string>()
                };

                // The MovieLens 100K dataset has 19 genre flags (positions 5–23)
                int genreStart = 5;
                int genreCount = Math.Min(p.Length - genreStart, GenreNames.Length);

                for (int i = 0; i < genreCount; i++)
                {
                    if (p[genreStart + i] == "1")
                        movie.Genres.Add(GenreNames[i]);
                }

                return movie;
            })
            .Where(m => m != null)
            .ToList();
        }


        public static List<Rating> LoadRatings(string path)
        {
            string file = Path.Combine(path, "u.data");
            return File.ReadLines(file).Select(line =>
            {
                var p = line.Split('\t');
                return new Rating
                {
                    UserId = int.Parse(p[0]),
                    MovieId = int.Parse(p[1]),
                    Score = double.Parse(p[2])
                };
            }).ToList();
        }

        // MovieLens 100k genres
       
        private static readonly string[] GenreNames = new[]
        {
    "Unknown", "Action", "Adventure", "Animation", "Children's", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
};

    }

    public static class ReportController
    {
        public static void GenerateReports(
            List<User> users,
            List<Movie> movies,
            List<Rating> ratings,
            string outputDir)
        {
            Directory.CreateDirectory(outputDir);

            // Report dictionary
            var reports = new Dictionary<string, List<ReportRow>>();

            // 1️⃣ General top 10
            reports["Top10_All"] = GetTopMovies(ratings, movies, 10);

            // 2️⃣ Gender based
            var maleUsers = users.Where(u => u.Gender == "M").Select(u => u.UserId).ToHashSet();
            var femaleUsers = users.Where(u => u.Gender == "F").Select(u => u.UserId).ToHashSet();
            reports["Top10_Male"] = GetTopMovies(ratings, movies, 10, maleUsers);
            reports["Top10_Female"] = GetTopMovies(ratings, movies, 10, femaleUsers);

            // 3️⃣ Genre-based (only selected genres)
            var targetGenres = new[] { "Action", "Drama", "Comedy", "Fantasy" };
            foreach (var genre in targetGenres)
            {
                var genreMovieIds = movies
                    .Where(m => m.Genres.Contains(genre))
                    .Select(m => m.MovieId)
                    .ToHashSet();

                var genreRatings = ratings.Where(r => genreMovieIds.Contains(r.MovieId)).ToList();
                reports[$"Top10_{genre}"] = GetTopMovies(genreRatings, movies, 10);
            }

            // 4️⃣ Age-based
            var under18 = users.Where(u => u.Age < 18).Select(u => u.UserId).ToHashSet();
            var age18to29 = users.Where(u => u.Age >= 18 && u.Age < 30).Select(u => u.UserId).ToHashSet();
            var age30plus = users.Where(u => u.Age >= 30).Select(u => u.UserId).ToHashSet();

            reports["Top10_Age_Under18"] = GetTopMovies(ratings, movies, 10, under18);
            reports["Top10_Age_18to29"] = GetTopMovies(ratings, movies, 10, age18to29);
            reports["Top10_Age_30plus"] = GetTopMovies(ratings, movies, 10, age30plus);

            // Save all 10 reports
            foreach (var r in reports)
                ReportView.SaveCSV(r.Key, r.Value, outputDir);
        }

        private static List<ReportRow> GetTopMovies(List<Rating> ratings, List<Movie> movies, int topN, HashSet<int> userFilter = null)
        {
            var filtered = userFilter == null ? ratings : ratings.Where(r => userFilter.Contains(r.UserId));
            var query = filtered.GroupBy(r => r.MovieId)
                .Select(g => new ReportRow
                {
                    MovieId = g.Key,
                    Title = movies.First(m => m.MovieId == g.Key).Title,
                    Avg = g.Average(x => x.Score),
                    Count = g.Count()
                })
                .Where(x => x.Count >= (userFilter != null && userFilter.Count < 100 ? 5 : 20))

                .OrderByDescending(x => x.Avg)
                .ThenByDescending(x => x.Count)
                .Take(topN)
                .ToList();

            return query;
        }
    }
}
