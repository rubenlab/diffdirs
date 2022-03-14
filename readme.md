# https://github.com/rubenlab/diffdirs

## Design goals

There are many existing directory comparison tools.

But most of them require two directories are logically connected, either on the same machine or able to be connected via ssh.

And the comparison is done at one time. If the comparison is between two large data sets, if there is a little abnormality in the comparison process, all the work will be wasted.

The purpose of this project is to design a comparison tool that can efficiently compare very large data sets, does not require the directories to be compared to be connected to each other, and can continue to execute at the interruption point after encountering an abnormal interruption.

## Usage

### Build:

Change directory to the project directory, run `go build .`

### Execution

Create a "config.yml" file under the working directory, parameters refer to the "config-test.yml" file.

Run `go diffdirs` to generate a folder information database. Execute this command in the two directories to be compared to generate two db files.

You can run the program in the background with the -d command: `go diffdirs -d`

After you get two db files, for example, source.db and target.db.

Use target.db as the db parameter in the configuration file. Execute the following command to generate the comparison result:

`go diffdirs -diff source.db`

The "diffresult.csv" file will be generated in the working directory.

## How it works

The program traverses the folder and records the file's checksum or file length (configurable) into a local database.

Compare the difference of two local databases to get the difference of the folders.

If the file checksum calculation is configured, the program uses multiple goroutines to calculate the checksum.

## Configuration

### db

Path to the local database

### first-run

If configured to false, the program will check whether the file already exists in the database, and skip the file if it exists.

### workers

Number of goroutines to calculate checksum.

### checksum

Whether to calculate the checksum, otherwise just log the file size.

### dirs

Directory key-value pair, which can compare multiple directories on two servers at one time.

## License

This project is licensed under the terms of the MIT license.