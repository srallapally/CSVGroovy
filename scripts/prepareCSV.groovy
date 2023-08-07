@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

def INPUTFILE = 'REPLACE WITH INPUT FILE PATH'

File csvFile = new File (INPUTFILE)
if (!csvFile.exists()) {
    throw new Exception("File not found: " + INPUTFILE)
}
def header = ['User_Name', 'User_First_Name', 'User_Last_Name', 'Groups']
def csvContent = csvFile.text
def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
def newData = csvData.collect { row ->
    [User_Name: row.User_Name, User_First_Name: row.User_First_Name, User_Last_Name: row.User_Last_Name, GROUP_NAME: row.GROUP_NAME]
}
// Sort by 'User_Name'.
newData.sort { a, b -> a.User_Name <=> b.User_Name }

// Group by 'User_Name' and collect 'GROUP_NAME' into 'Groups'.
def groupedData = newData.groupBy { it.User_Name }.collect { userName, rows ->
    def groups = rows*.GROUP_NAME.join(', ')
    def list = []
    //list = groups.split("\\s*,\\s*")
    def test = groups.replaceAll(/[\[\]"]/,'').trim()
    //list = groups.replaceAll(/[\[\]"]/,'').trim().split("\\s*,\\s*")
    //rows[0] + [Groups: list]
    rows[0] + [Groups: test]
}
//println groupedData
// Write updated data to CSV
def outputFileName = 'REPLACE WITH OUTPUT FILE PATH'
File outfile = new File(outputFileName)
new File(outputFileName).withWriter { writer ->
    writer.writeLine(header.collect { "\"${it}\"" }.join(','))
}
outfile.withWriterAppend { writer ->
    groupedData.each { record ->
        writer.writeLine(header.collect { "\"${record[it]}\"" }.join(','))
    }
}

csvFile = new File (INPUTFILE)
if (!csvFile.exists()) {
    throw new Exception("File not found: " + INPUTFILE)
}
csvContent = csvFile.text
csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
newData = csvData.collect { row ->
    [GROUP_NAME: row.GROUP_NAME, GROUP_DESC: row.GROUP_DESC]
}

// Eliminate duplicates from newData using GROUP_NAME as the key
def uniqueGroupData = newData.unique().sort { a, b -> a.GROUP_NAME <=> b.GROUP_NAME }
outputFileName = 'REPLACE WITH OUTPUT FILE PATH'
outfile = new File(outputFileName)
header = ['GROUP_NAME', 'GROUP_DESC']
new File(outputFileName).withWriter { writer ->
    writer.writeLine(header.collect { "\"${it}\"" }.join(','))
}
outfile.withWriterAppend { writer ->
    newData.each { record ->
        writer.writeLine(header.collect { "\"${record[it]}\"" }.join(','))
    }
}