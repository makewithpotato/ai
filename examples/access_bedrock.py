from anthropic import AnthropicBedrock
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

client = AnthropicBedrock(
    # 아래 키를 제공하거나 ~/.aws/credentials 또는 "AWS_SECRET_ACCESS_KEY"와 "AWS_ACCESS_KEY_ID" 환경 변수와 같은
    # 기본 AWS 자격 증명 제공자를 사용하여 인증하세요.
    aws_access_key= os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_key= os.getenv("AWS_SECRET_ACCESS_KEY"),
    # aws_region은 요청이 전송되는 aws 지역을 변경합니다. 기본적으로 AWS_REGION을 읽고,
    # 없는 경우 us-east-1이 기본값입니다. 참고로 ~/.aws/config에서 지역을 읽지 않습니다.
    aws_region=os.getenv("AWS_DEFAULT_REGION"),
)

message = client.messages.create(
    model=os.getenv("CLAUDE_MODEL_ID"),
    max_tokens=256,
    messages=[{"role": "user", "content": "안녕, 현재 대한민국의 메인 뉴스에 대해서 알려 줘"}]
)
print(message.content)
