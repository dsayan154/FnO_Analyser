import boto3, logging, yaml, json
from botocore.exceptions import ClientError

class AWS:
  def __init__(self, *args) -> None:
    __setClientSwitches = {
      's3': self.__setS3Client,
      'ssm': self.__setSSMClient,
      'default': self.__wrongResourceType
    }
    for arg in args:
      __setClientSwitches.get(arg, __setClientSwitches['default'])()
  
  def __setS3Client(self):
    self.s3 = boto3.resource('s3')
  
  def __setSSMClient(self):
    self.ssm = boto3.client('ssm')
  
  def __wrongResourceType(self):
    raise ValueError('wrong aws resource type passed to initialize the AWS class')
  
  def getDetailsFromSSM(self, parameterName: str) -> str:
    data = self.ssm.get_parameter(Name=parameterName)['Parameter']['Value']
    return str(data)

  def getYamlDataFromSSM(self, parameterName: str) -> dict:
    config = yaml.safe_load(self.getDetailsFromSSM(parameterName))
    return config
  
  def getDataFroms3(self, s3BucketName: str, s3BucketFileName: str):
    logging.info('getting data from s3')
    s3Object = self.s3.Object(s3BucketName, s3BucketFileName)
    resp = s3Object.get()
    if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info('fetched data from s3')
    else:
        logging.error('error getting data from s3')
    return str(resp['Body'].read().decode('utf-8'))
  
  def getJsonDataFromS3(self, s3BucketName:str, s3BucketFileName:str):
    s3JsonData = json.loads(self.getDataFroms3(s3BucketName, s3BucketFileName))
    return s3JsonData
  
  def getDataFromSecretsManager(self, secretName: str, region: str) -> str:
    region_name = region
    secret_name = secretName
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            # decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            raise ValueError('We are not dealing with binary secrets now.')
    return secret
  
  def uploadJsonToS3(self, data:dict, s3BucketName:str, s3FileName:str): 
    s3Object = self.s3.Object(s3BucketName, s3FileName)
    result = s3Object.put(Body = json.dumps(data).encode('UTF-8'))
    response = result.get('ResponseMetadata').get('HTTPStatusCode')
    if response != 200:
      logging.error('File not uploaded')
    else:
      logging.info('File uploaded successfully')