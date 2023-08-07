import com.opencsv.CSVReader
import com.opencsv.CSVWriter

def inputFile = new File('REPLACE WITH INPUT FILE PATH')
def outputFile = new File('REPLACE WITH OUTPUT FILE PATH')

def reader = new CSVReader(new FileReader(inputFile))
List<String[]> allRows = reader.readAll()

// Remove blank lines
allRows.removeAll { row -> row.every { it.isBlank() } }
allRows.remove(0)
// Get the index of groupname and groupdesc columns
int groupnameIdx = allRows[1].findIndexOf { it == 'GROUP_NAME' }
int groupdescIdx = allRows[1].findIndexOf { it == 'GROUP_DESC' }

// Extract groupname and groupdesc into a new List
def groupData = allRows.collect { row -> [row[groupnameIdx], row[groupdescIdx]] } as List<String[]>

// Eliminate duplicates from groupData using GROUP_NAME as the key
def uniqueGroupData = groupData.unique { it[0] } as List<String[]>

CSVWriter writer = new CSVWriter(new FileWriter(outputFile))
writer.writeNext(['GROUP_NAME', 'GROUP_DESC'] as String[])
uniqueGroupData.each { row ->
    writer.writeNext( row as String[]) }

// close reader and writer
reader.close()
writer.close()
