@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv
import com.xlson.groovycsv.CsvParser

def data = '''
"User_Name","User_First_Name","User_Last_Name","GROUP_NAME","GROUP_DESC"
"100009","MARY","LYNCH","DEFAULT","Inquiry - Check Request - Code & Authorize"
"100013","ANA","GOUGHAN","DEFAULT","Inquiry - Check Request - Code & Authorize"
"100039","MARTA","BARRIOS","DEFAULT","Inquiry - Check Request - Code & Authorize"
"100325","MICHELLE","HOMOKY","DEFAULT","Inquiry - Check Request - Code & Authorize"
"100330","REGLA","CABANAS-CALERO","COORDINATOR_GRP","Coordinator Group"
"100330","REGLA","CABANAS-CALERO","DEFAULT","Inquiry - Check Request - Code & Authorize"
"100330","REGLA","CABANAS-CALERO","GSFAP50M","Special Funding AP Supervisors"
"100330","REGLA","CABANAS-CALERO","SS_INVOICE","SelfService Invoice Creation"
'''

def csvContent = new File('/Users/sanjay.rallapally/Downloads/ap-tungsten-entitlements.csv').text
def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)

def newData = csvData.collect { row ->
    [User_Name: row.User_Name, User_First_Name: row.User_First_Name, User_Last_Name: row.User_Last_Name, GROUP_NAME: row.GROUP_NAME]
}


// Sort by 'User_Name'.
newData.sort { a, b -> a.User_Name <=> b.User_Name }

// Group by 'User_Name' and collect 'GROUP_NAME' into 'Groups'.
def groupedData = newData.groupBy { it.User_Name }.collect { userName, rows ->
    def groups = rows*.GROUP_NAME.join(', ')
    rows[0] + [Groups: groups]
}

// Print the results.
groupedData.each { println it }
