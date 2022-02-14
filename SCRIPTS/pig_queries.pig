--########Creation Des tables
A = LOAD '/user/student/data/miniprojet/movies.dat' USING PigStorage('\n');
raw_movies = FOREACH A GENERATE FLATTEN(STRSPLIT($0, '::'));

movies_details = FOREACH raw_movies
GENERATE ((INT)$0) AS MovieID, $1 AS Title, $2 AS Genres;

A = LOAD '/user/student/data/miniprojet/users.dat' USING PigStorage('\n');
raw_users = FOREACH A GENERATE FLATTEN(STRSPLIT($0, '::'));

users_details = FOREACH raw_users
GENERATE ((INT)$0) AS UserID , $1 AS Gender , ((INT)$2) AS Age , ((INT)$3) AS Occupation , $4 AS Zip_code ;

A = LOAD '/user/student/data/miniprojet/ratings.dat' USING PigStorage('\n');
raw_ratings = FOREACH A GENERATE FLATTEN(STRSPLIT($0, '::'));

ratings_details = FOREACH raw_ratings
GENERATE ((INT)$0) AS UserID , ((INT)$1) AS MovieID, ((INT)$2) AS Rating , ((INT)$3) AS TimesRating ;

--____________________________________________________________________________________________________________________

limited = LIMIT movies_details 10 ;
DUMP limited ;

limited = LIMIT users_details 10 ;
DUMP limited ;

limited = LIMIT ratings_details 10 ;
DUMP limited ;

--____________________________________________________________________________________________________________________
movies_ratings_filter = FILTER movie_details BY Genres matches '.*Action.*';
ratings_groupby = GROUP ratings_details BY MovieID;
rating_average= FOREACH ratings_groupby GENERATE AVG(ratings_details.Rating) AS avgRating;

movies_ratings_join = JOIN ratings_groupby BY $0, rating_average BY $0;

movies_ratings_join_order = ORDER movies_ratings_join BY $2 DESC;
movies_rations_result = LIMIT  movies_ratings_join_order 10;
STORE movies_rations_result INTO '/user/student/data/miniprojet/movies_ratings_result';
--____________________________________________________________________________________________________________________
coGroupMoviesRatings = COGROUP movies_details BY MovieID,ratings_details by MovieID;
DESCRIBE coGroupMoviesRatings;
res = LIMIT coGroupMoviesRatings 10;
--____________________________________________________________________________________________________________________

femaleUsers = FILTER users_details BY (Gender == 'F') AND (Age < 30) AND (Age >= 20);
limited = LIMIT femaleUsers 10 ;
DUMP limited ;
