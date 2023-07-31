@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.identityconnectors.framework.common.objects.filter.Filter
import org.identityconnectors.framework.common.objects.filter.EqualsFilter
import org.identityconnectors.framework.common.objects.filter.FilterBuilder
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter
import org.identityconnectors.framework.common.objects.Uid

def operation = operation as OperationType
def configuration = configuration as ScriptedConfiguration
def filter = filter as Filter
def log = log as Log
def objectClass = objectClass as ObjectClass
def options = options as OperationOptions

println "Entering " + operation + " Script"

def query = [:]
def queryFilter = 'true'


switch (objectClass) {
    case ObjectClass.ACCOUNT:
        // Handle the results

        def fileLocation = configuration.propertyBag.__ACCOUNT__.fileloc
        def resources = null
        if(null != fileLocation) {
            println "Loading " + fileLocation
            resources = loadCsvData(fileLocation)
        } else {
            resources = loadCsvData('/Users/sanjay.rallapally/Downloads/ap-tungsten-entitlements.csv')
        }
        if(filter != null){
            if(filter instanceof EqualsFilter){
                query = filter.attributeExpression as Map
                queryFilter = query.operation + ":" + query.left + ":" + query.right
            }
            else if(filter instanceof StartsWithFilter){
                //query = filter.attributeExpression as Map
                //queryFilter = query.operation + ":" + query.left + ":" + query.right
                println filter
                println filter.getName()
                println filter.getValue()
            }
        } else {
            if (null != options.getPageSize()) {
                String pagedResultsCookie = options.getPagedResultsCookie();
                String currentPagedResultsCookie = options.getPagedResultsCookie();
                println "Paged Search: pagedResultsCookie: " + pagedResultsCookie
                Integer pagedResultsOffset =
                        null != options.getPagedResultsOffset() ? Math.max(0, options
                                .getPagedResultsOffset()) : 0;
                final Integer pageSize = options.getPageSize();
                println resources.size()
                println "Paged Search: pagedResultsOffset: " + pagedResultsOffset + ", pageSize: " + pageSize
                if(options.pagedResultsCookie) {
                    lastHandledIndex = resources.findIndexOf { resource ->
                        resource.uid == new String(options.pagedResultsCookie.decodeBase64Url())
                    }
                } else if (options.pagedResultsOffset){
                    resources = resources.drop options.pagedResultsOffset
                }
                println 'After drop' + resources.size()
                def remainingPagedResults = resources.size() - pageSize
                resources = resources.subList 0, Math.min(pageSize, resources.size())
                println 'After subList' + resources.size()
                //if (remainingPagedResults > 0) {
                //    pagedResultsCookie = resources?.last().uid.bytes.encodeBase64Url().toString()
                //}
                //resources

                resources.each { row ->
                    handler {
                        uid row.User_Name
                        id  row.User_Name
                        attribute 'email', row.User_Name
                        attribute 'lastName', row.User_Last_Name
                        attribute 'firstName', row.User_First_Name
                        attribute 'userName', row.User_Name
                        attribute 'groups', row.Groups
                    }
                }
                return new SearchResult(pagedResultsCookie,-1);
            }
        }
    case "__GROUP__":
        break
    default:
        throw new UnsupportedOperationException(operation.name() + " operation of type:" +
                objectClass.objectClassValue + " is not supported.")
}

def getPage(HashMap data,int start, int page) {
    def end = start + page
    return data.values().toList().subList(start, Math.min(end, data.size()))
}

def loadCsvData (String fileName) {
    File csvFile = new File (fileName)
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
        rows[0] + [Groups: groups]
    }
    return groupedData
}

