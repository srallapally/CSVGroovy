@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

def loadCsvData = {
    //def csvFile = new File('/Users/sanjay.rallapally/Downloads/ap-tungsten-entitlements.csv')
    def csvContent = new File('/Users/sanjay.rallapally/Downloads/ap-tungsten-entitlements.csv').text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    //def csvData = parseCsv(new FileReader('/Users/sanjay.rallapally/Downloads/ap-tungsten-entitlements.csv'), separator: ',', readFirstLine: false)
    def map = [:]

    csvData.each { row ->
        def userId = row[0]  // Assuming the userid is the first column
        map[userId] = row
    }
    def sortedMap = map.sort {it.key}
    return sortedMap
}

def searchUserId = { userIdToFind, csvMap ->
    return csvMap[userIdToFind]
}

def csvMap = loadCsvData()

def userRow = searchUserId('100330', csvMap)  // Replace 'userid_to_find' with the actual id you are searching

if (userRow) {
    println("Found user: ${userRow}")
} else {
    println("User not found.")
}

