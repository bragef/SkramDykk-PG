# SkramDykk-PG

SkramDykk is an open source set of tools to read data from a CTD,
store, analyse and visusalise the data.  The goal of the project is
that we can produce better 2D and 3D visualizations of data from dives
so that students and teachers better can understand the dynamics and
biology of oceans.

The tools will run on a Linux server and consists of several tools to
fetch, manage data, ownload and visualize them.

This version (SkramDykk-PG) is a rewrite of the original SkramDykk by
Nils Jacob Berland, where the original MongoDB backend is replaced by
a PostgreSQL database.

Other improvements include
* New download page, where data are downloadable as CSV or Excel
* Better performance
* More time resampling intervals available
* Weather data (air temperature, windspeed, wind direction) available for dwnload 

At present the server is located at
[ektedata.uib.no/gabrieldata](https://ektedata.uib.no/gabrieldata/)

The work was supported by:

* Amalie Skram VGS - [www.asvgs.no](http://www.asvgs)
* Universitetet i Bergen - [www.uib.no](http://www.uib.no)


![Screenshot](https://github.com/njberland/SkramDykk/blob/master/screenshots/Screenshot%20temperature.png "Screenshot")

![Gabriel](https://github.com/njberland/SkramDykk/blob/master/screenshots/gabriel.JPG "Gabriel")



