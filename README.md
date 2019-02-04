# Drive Time Service

## Overview

This is a simple Windows service which determines drive times using [Google's Distance Matrix API](https://developers.google.com/maps/documentation/distance-matrix/start). It's wrapped by the library [Topshelf](http://topshelf-project.com/) which allows the service to run as both a console app and Windows service.

The application uses `log4net` for logging, configuration is stored in `log4net.config`. By default it uses a console log, rolling file log and writes to the Windows event log.

## Configuration

The project uses [app.config transformations](https://marketplace.visualstudio.com/items?itemName=GolanAvraham.ConfigurationTransform), i.e. the config file being used depends on the build target.

There is also a `config.json` file which contains a configuration options used by the `DriveTime` class for handling requests to the DriveTime API. Note that this configuration file contains a placeholder for the API key which is substituted in on lines 33-36 of Program.cs.

# Workflow

`Program.cs` initialises the service before handing off to `Worker.cs` which firstly calls `DriveTime_Request_xProcess` which processes any unprocessed `DriveTime_Request` rows. It then gets a batch of pending rows from `DriveTime_qListPending`. For each row in the batch the data is passed to `DriveTime.LookupAsync` which returns a response JSON, the response is then inserted into `DriveTime_Request` via a call to `DriveTime_Request_xInsert`. Once all rows have been processed the service will sleep for `SleepDurationMs` (default 5 minutes).