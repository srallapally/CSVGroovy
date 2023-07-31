import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.common.security.GuardedByteArray
import org.identityconnectors.common.security.GuardedString
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptionInfoBuilder
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos
import org.identityconnectors.framework.common.objects.PredefinedAttributeInfos
import org.identityconnectors.framework.spi.operations.AuthenticateOp
import org.identityconnectors.framework.spi.operations.ResolveUsernameOp
import org.identityconnectors.framework.spi.operations.SchemaOp
import org.identityconnectors.framework.spi.operations.ScriptOnConnectorOp
import org.identityconnectors.framework.spi.operations.ScriptOnResourceOp
import org.identityconnectors.framework.spi.operations.SearchOp
import org.identityconnectors.framework.spi.operations.SyncOp
import org.identityconnectors.framework.spi.operations.TestOp

import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.MULTIVALUED
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.NOT_CREATABLE
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.NOT_READABLE
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.NOT_RETURNED_BY_DEFAULT
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.NOT_UPDATEABLE
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.REQUIRED

def operation = operation as OperationType
def configuration = configuration as ScriptedConfiguration
def log = log as Log

builder.schema({
    objectClass {
        type ObjectClass.ACCOUNT_NAME
        attributes {
            userName String.class, REQUIRED
            email REQUIRED, MULTIVALUED
            __ENABLE__ Boolean.class
            createDate  NOT_CREATABLE, NOT_UPDATEABLE
            lastModified Long.class, NOT_CREATABLE, NOT_UPDATEABLE
            firstName String.class, REQUIRED
            lastName String.class, REQUIRED
            groups String.class MULTIVALUED
        }

    }
    objectClass {
        type ObjectClass.GROUP_NAME
        attributes {
            groupName String.class, REQUIRED
            groupDisplayName String.class, REQUIRED
            member String.class, MULTIVALUED
        }
        // ONLY CRUD
    }

    operationOption {
        name "notify"
        disable AuthenticateOp, ResolveUsernameOp, SchemaOp, ScriptOnConnectorOp, ScriptOnResourceOp, SyncOp, TestOp
    }
    operationOption {
        name "force"
        type Boolean
    }

    defineOperationOption OperationOptionInfoBuilder.buildPagedResultsCookie(), SearchOp
    defineOperationOption OperationOptionInfoBuilder.buildPagedResultsOffset(), SearchOp
    defineOperationOption OperationOptionInfoBuilder.buildPageSize(), SearchOp
    defineOperationOption OperationOptionInfoBuilder.buildSortKeys(), SearchOp
    defineOperationOption OperationOptionInfoBuilder.buildRunWithUser()
    defineOperationOption OperationOptionInfoBuilder.buildRunWithPassword()
    }
)

