import boto3
import csv
import time

def get_var_char_values(d):
    return [obj['VarCharValue'] for obj in d['Data']]
    
def query_results(session, params, wait = True):    
    client = session.client('athena', region_name=params["region"])
    
    ## This function executes the query and returns the query execution ID
    response_query_execution_id = client.start_query_execution(
        QueryString = params['query'],
        ExecutionParameters = [params['file_name']],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

    if not wait:
        return response_query_execution_id['QueryExecutionId']
    else:
        response_get_query_details = client.get_query_execution(
            QueryExecutionId = response_query_execution_id['QueryExecutionId']
        )
        status = 'RUNNING'
        iterations = 360 # 30 mins

        while (iterations > 0):
            iterations = iterations - 1
            response_get_query_details = client.get_query_execution(
            QueryExecutionId = response_query_execution_id['QueryExecutionId']
            )
            status = response_get_query_details['QueryExecution']['Status']['State']
            
            if (status == 'FAILED') or (status == 'CANCELLED') :
                return False, False

            elif status == 'SUCCEEDED':
                location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                ## Function to get output results
                response_query_result = client.get_query_results(
                    QueryExecutionId = response_query_execution_id['QueryExecutionId']
                )
                result_data = response_query_result['ResultSet']
                
                result = []

                if len(response_query_result['ResultSet']['Rows']) > 1 :
                    header = response_query_result['ResultSet']['Rows'][0]
                    for r in response_query_result['ResultSet']['Rows'][1:]:
                        columnindex = 0
                        for columnvalue in r['Data']:
                            result.append(r['Data'][columnindex]['VarCharValue'])
                            columnindex += 1
                    return result
                else:
                    return None
        	else:
                time.sleep(1)

        return False