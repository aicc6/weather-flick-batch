# Weather Flick 보안 및 인증 시스템 진화 과정

## 목차
1. [인증 시스템 진화](#1-인증-시스템-진화)
2. [권한 관리 시스템](#2-권한-관리-시스템)
3. [보안 강화 과정](#3-보안-강화-과정)
4. [데이터 보호](#4-데이터-보호)
5. [인증 관련 버그 및 해결](#5-인증-관련-버그-및-해결)

---

## 1. 인증 시스템 진화

### 1.1 기본 JWT 인증 (초기 구현)

초기에는 간단한 JWT 기반 인증으로 시작했습니다.

```python
# app/auth.py (초기 버전)
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    """JWT 액세스 토큰 생성"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
```

### 1.2 OAuth 2.0 통합 (2025-07-01)

Google OAuth 통합을 통해 소셜 로그인 기능을 추가했습니다.

```python
# Migration: 2a45c1ef874e_add_google_oauth_fields_to_user_model.py
def upgrade() -> None:
    """Google OAuth 필드 추가"""
    op.add_column('users', sa.Column('google_id', sa.String(), nullable=True))
    op.add_column('users', sa.Column('auth_provider', sa.String(), nullable=True))
    op.alter_column('users', 'hashed_password',
               existing_type=sa.VARCHAR(),
               nullable=True)  # OAuth 사용자는 비밀번호가 없을 수 있음
    op.create_unique_constraint(None, 'users', ['google_id'])
```

Google OAuth 서비스 구현:
```python
# app/services/google_oauth_service.py
class GoogleOAuthService:
    """구글 OAuth 인증 서비스"""
    
    async def verify_google_token(self, token: str) -> dict[str, Any]:
        """구글 ID 토큰 검증"""
        try:
            idinfo = id_token.verify_oauth2_token(
                token, requests.Request(), self.client_id
            )
            
            # 토큰 유효성 검증
            if idinfo["aud"] != self.client_id:
                raise ValueError("Wrong audience.")
                
            if idinfo["iss"] not in ["accounts.google.com", "https://accounts.google.com"]:
                raise ValueError("Wrong issuer.")
                
            return {
                "google_id": idinfo["sub"],
                "email": idinfo["email"],
                "name": idinfo.get("name", ""),
                "picture": idinfo.get("picture", ""),
                "email_verified": idinfo.get("email_verified", False),
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid token: {str(e)}")
```

### 1.3 토큰 관리 전략 변화

#### 초기: 단순 액세스 토큰
```python
# 초기 설정
ACCESS_TOKEN_EXPIRE_MINUTES = 30
```

#### 개선: 토큰 만료 시간 확장 및 리프레시 토큰 개념 도입
```python
# app/config.py
access_token_expire_minutes: int = 30
refresh_token_expire_minutes: int = 10080  # 7 days
```

### 1.4 세션 관리 방식

#### 백엔드: Stateless JWT 방식
```python
def get_current_user(token=Depends(bearer_scheme), db: Session = Depends(get_db)):
    """현재 사용자 조회"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    token_data = verify_token(token.credentials, credentials_exception)
    user = db.query(User).filter(User.email == token_data.email).first()
    if user is None:
        raise credentials_exception
    return user
```

#### 프론트엔드: LocalStorage 기반 토큰 관리
```javascript
// src/store/api/baseQuery.js
export const baseQuery = fetchBaseQuery({
  baseUrl: getBaseUrl(),
  prepareHeaders: (headers, { endpoint }) => {
    const authRequiredEndpoints = [
      'getMe', 'updateProfile', 'changePassword', 'logout', 'withdraw',
      'createTravelPlan', 'getUserPlans', 'updateTravelPlan', 'deleteTravelPlan'
    ]
    
    if (authRequiredEndpoints.includes(endpoint)) {
      const token = localStorage.getItem(STORAGE_KEYS.ACCESS_TOKEN)
      if (token) {
        headers.set('authorization', `Bearer ${token}`)
      }
    }
    return headers
  },
})
```

### 1.5 리프레시 토큰 구현 (계획)

현재 리프레시 토큰은 설정만 되어있고 실제 구현은 예정되어 있습니다.

---

## 2. 권한 관리 시스템

### 2.1 사용자 역할 정의 (2025-07-03)

```python
# Migration: fb2ec0935be9_add_role_column_to_users.py
class UserRole(enum.Enum):
    USER = "USER"
    ADMIN = "ADMIN"

def upgrade() -> None:
    """사용자 역할 추가"""
    userrole_enum = sa.Enum('USER', 'ADMIN', name='userrole')
    userrole_enum.create(op.get_bind())
    op.add_column('users', sa.Column('role', userrole_enum, nullable=True))
    op.execute("UPDATE users SET role = 'USER' WHERE role IS NULL")
    op.alter_column('users', 'role', nullable=False)
```

### 2.2 RBAC 구현

#### 사용자 서비스 (weather-flick-back)
```python
def get_current_admin_user(current_user: User = Depends(get_current_active_user)):
    """현재 관리자 사용자 조회"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Not enough permissions"
        )
    return current_user
```

#### 관리자 서비스 (weather-flick-admin-back)
```python
# app/auth/dependencies.py
async def require_super_admin(
    current_admin: Admin = Depends(get_current_active_admin)
) -> Admin:
    """슈퍼관리자 권한 필요"""
    # 슈퍼관리자 판별: 이메일이 admin@weatherflick.com인 경우
    if current_admin.email != "admin@weatherflick.com":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="슈퍼관리자 권한이 필요합니다"
        )
    return current_admin
```

### 2.3 권한 체크 미들웨어

```python
# 인증이 필요한 엔드포인트 보호
@router.post("/change-password")
async def change_password(
    password_change: PasswordChange,
    current_user: User = Depends(get_current_active_user),  # 인증 필수
    db: Session = Depends(get_db),
):
    """비밀번호 변경"""
    # 로직 구현
```

### 2.4 관리자 권한 시스템

관리자는 별도의 테이블과 상태 관리를 가집니다:

```python
class AdminStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    LOCKED = "LOCKED"

class Admin(Base):
    __tablename__ = "admins"
    admin_id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    status = Column(Enum(AdminStatus), default=AdminStatus.ACTIVE)
    last_login_at = Column(DateTime)
```

---

## 3. 보안 강화 과정

### 3.1 비밀번호 정책

강력한 비밀번호 정책 구현:
```python
def check_password_strength(password: str) -> dict:
    """비밀번호 강도 검사"""
    errors = []
    if len(password) < 8:
        errors.append("Password must be at least 8 characters long")
    if not any(c.isupper() for c in password):
        errors.append("Password must contain at least one uppercase letter")
    if not any(c.islower() for c in password):
        errors.append("Password must contain at least one lowercase letter")
    if not any(c.isdigit() for c in password):
        errors.append("Password must contain at least one number")
    if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
        errors.append("Password must contain at least one special character")

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "strength": "strong" if len(errors) == 0 else "weak",
    }
```

보안 강화된 임시 비밀번호 생성:
```python
def generate_temporary_password(length: int = 12) -> str:
    """보안 강화된 임시 비밀번호 생성"""
    import secrets
    import string
    
    # 각 문자 유형별로 최소 1개씩 포함
    lowercase = string.ascii_lowercase
    uppercase = string.ascii_uppercase
    digits = string.digits
    special_chars = "!@#$%^&*"
    
    password_chars = [
        secrets.choice(lowercase),
        secrets.choice(uppercase),
        secrets.choice(digits),
        secrets.choice(special_chars)
    ]
    
    # 나머지 자리는 모든 문자에서 랜덤 선택
    all_chars = lowercase + uppercase + digits + special_chars
    for _ in range(length - 4):
        password_chars.append(secrets.choice(all_chars))
    
    # 문자 순서 섞기 (패턴 예측 방지)
    secrets.SystemRandom().shuffle(password_chars)
    
    return ''.join(password_chars)
```

### 3.2 API 보안

#### CORS 설정
```python
# main.py
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 프로덕션에서는 특정 도메인으로 제한
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# config.py
@property
def cors_origins_list(self) -> list[str]:
    """Get CORS origins as list."""
    if self.is_production:
        return [self.frontend_url]
    return self.cors_origins
```

#### Rate Limiting (계획 중)
현재는 구현되지 않았지만, 향후 FastAPI-Limiter 등을 활용한 Rate Limiting 구현 예정

### 3.3 SQL Injection 방어

SQLAlchemy ORM 사용으로 자동 방어:
```python
# 안전한 쿼리 사용
user = db.query(User).filter(User.email == email).first()

# 파라미터화된 쿼리
existing_user = (
    db.query(User)
    .filter(
        func.lower(User.nickname) == func.lower(user.nickname),
        User.is_active == True
    )
    .first()
)
```

### 3.4 XSS/CSRF 방어

#### XSS 방어
- React의 자동 이스케이핑 활용
- 사용자 입력 검증 및 살균

```python
# 입력값 전처리
if user.nickname and user.nickname.strip():
    user.nickname = user.nickname.strip()
```

#### CSRF 방어
- JWT 토큰 기반 인증으로 CSRF 공격 자동 방어
- SameSite 쿠키 설정 (향후 쿠키 사용 시)

---

## 4. 데이터 보호

### 4.1 개인정보 암호화

#### 비밀번호 해싱
```python
# Bcrypt 사용
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    """비밀번호 해싱"""
    return pwd_context.hash(password)
```

#### 회원 탈퇴 시 이메일 마스킹
```python
# 사용자 계정 비활성화 (소프트 삭제) 및 이메일 마스킹
current_user.is_active = False
current_user.email = f"deleted_{current_user.user_id}_{current_user.email}"
```

### 4.2 API 키 관리

#### 환경 변수 사용
```python
# config.py
secret_key: str = os.getenv("JWT_SECRET_KEY", "")
google_client_secret: str = os.getenv("GOOGLE_CLIENT_SECRET", "")

@validator("secret_key")
def secret_key_must_be_set(cls, v: str) -> str:
    """Validate that secret key is set."""
    if not v:
        raise ValueError("JWT_SECRET_KEY must be set")
    return v
```

#### 배치 시스템의 API 키 관리
```python
# weather-flick-batch/config/settings.py
@dataclass
class APIConfig:
    """API 설정"""
    kto_api_key: str
    kto_base_url: str
    kma_api_key: str
    timeout: int = 30
    retry_count: int = 3
```

### 4.3 환경변수 보안

필수 환경변수 검증:
```python
# 설정 인스턴스 생성
settings = Settings()

# 필수 환경 변수 검증
if not settings.secret_key:
    raise ValueError("JWT_SECRET_KEY environment variable is required")

if not settings.database_url:
    raise ValueError("DATABASE_URL environment variable is required")
```

### 4.4 로그 보안

사용자 활동 로깅:
```python
def log_user_activity(
    db: Session,
    user_id,
    activity_type: str,
    description: str = None,
    ip_address: str = None,
    user_agent: str = None,
):
    """사용자 활동 로깅"""
    details = {}
    if description:
        details["description"] = description
    if ip_address:
        details["ip_address"] = ip_address
    if user_agent:
        details["user_agent"] = user_agent

    activity = UserActivity(
        user_id=user_id, 
        activity_type=activity_type, 
        details=details
    )
    db.add(activity)
    db.commit()
```

---

## 5. 인증 관련 버그 및 해결

### 5.1 세션 만료 문제

#### 문제
- JWT 토큰 만료 시 갑작스러운 로그아웃

#### 해결
```javascript
// baseQuery.js
export const baseQueryWithReauth = async (args, api, extraOptions) => {
  let result = await baseQuery(args, api, extraOptions)

  // 401 에러 처리
  if (result.error && result.error.status === 401) {
    console.warn('Unauthorized access - redirecting to login')
    handle401Error()
  }

  return result
}

const handle401Error = () => {
  localStorage.removeItem(STORAGE_KEYS.ACCESS_TOKEN)
  localStorage.removeItem(STORAGE_KEYS.USER_INFO)

  // 현재 페이지가 로그인 페이지가 아닌 경우에만 리다이렉트
  if (window.location.pathname !== '/login') {
    window.location.href = '/login'
  }
}
```

### 5.2 토큰 갱신 문제

#### 문제
- OAuth 로그인 후 토큰 상태 동기화 문제

#### 해결
```javascript
// AuthContextRTK.jsx
const handleGoogleAuthSuccess = useCallback(
  (userInfo, accessToken) => {
    // 토큰과 사용자 정보 저장
    tokenManager.setToken(accessToken, userInfo)

    // RTK Query 캐시 갱신
    refetch()
  },
  [refetch, tokenManager],
)
```

### 5.3 OAuth 통합 이슈

#### 문제
- Google OAuth 콜백 처리 시 보안 문제

#### 해결
임시 인증 코드 방식 도입:
```python
# 임시 인증 코드 저장소 (프로덕션에서는 Redis 사용 권장)
temp_auth_store: dict[str, dict] = {}

def store_temp_auth(code: str, data: dict, ttl_minutes: int = 10):
    """임시 인증 데이터 저장"""
    expire_time = datetime.now() + timedelta(minutes=ttl_minutes)
    temp_auth_store[code] = {"data": data, "expire_time": expire_time}

@router.get("/google/callback")
async def google_callback(code: str, state: str, request: Request = None):
    """구글 OAuth 콜백 처리 - 임시 코드 생성"""
    # 임시 인증 코드 생성
    temp_code = secrets.token_urlsafe(32)
    
    # 구글 인증 데이터를 임시 저장 (5분 TTL)
    store_temp_auth(
        temp_code,
        {
            "google_code": code,
            "state": state,
            "ip_address": request.client.host if request else None,
            "user_agent": request.headers.get("User-Agent") if request else None,
        },
    )
    
    # 프론트엔드로 임시 코드와 함께 리다이렉트
    frontend_url = f"{settings.frontend_url}/auth/google/callback?auth_code={temp_code}"
    return RedirectResponse(url=frontend_url, status_code=302)
```

### 5.4 중복 가입 방지

#### 문제
- 동일 이메일/닉네임으로 중복 가입 가능

#### 해결
```python
# 이메일 중복 확인 (활성 사용자만)
existing_email = (
    db.query(User)
    .filter(
        User.email == user.email,
        User.is_active == True
    )
    .first()
)

# 닉네임 중복 확인 (대소문자 무시, 활성 사용자만)
from sqlalchemy import func
existing_nickname = (
    db.query(User)
    .filter(
        func.lower(User.nickname) == func.lower(user.nickname),
        User.is_active == True
    )
    .first()
)
```

---

## 향후 개선 계획

1. **리프레시 토큰 구현**
   - 액세스 토큰 자동 갱신
   - 리프레시 토큰 로테이션

2. **2FA (Two-Factor Authentication)**
   - TOTP 기반 2차 인증
   - SMS/이메일 인증

3. **Rate Limiting**
   - API 엔드포인트별 요청 제한
   - IP 기반 차단

4. **보안 감사 로그**
   - 상세한 보안 이벤트 기록
   - 이상 행동 탐지

5. **암호화 강화**
   - 민감한 데이터 필드 암호화
   - 전송 중 암호화 (TLS/SSL)

6. **세션 관리 개선**
   - Redis 기반 세션 저장소
   - 디바이스별 세션 관리