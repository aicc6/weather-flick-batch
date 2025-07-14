"""날씨 변경 감지 및 비교 서비스"""
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, date
from dataclasses import dataclass
import json
from app.core.logger import get_logger

logger = get_logger(__name__)

@dataclass
class WeatherChange:
    """날씨 변경 정보"""
    date: date
    field: str  # 변경된 필드명
    old_value: Any
    new_value: Any
    change_type: str  # 'temperature', 'rain', 'condition' 등
    severity: str  # 'info', 'warning', 'critical'
    description: str

class WeatherComparisonService:
    """날씨 비교 및 변경 감지 서비스"""
    
    # 중요 날씨 변경 임계값
    TEMP_CHANGE_THRESHOLD = 5.0  # 5도 이상 변화
    RAIN_CHANGE_THRESHOLD = 30  # 30% 이상 강수확률 변화
    
    # 날씨 상태 우선순위 (숫자가 높을수록 악천후)
    WEATHER_SEVERITY = {
        'sunny': 1,
        'cloudy': 2,
        'overcast': 3,
        'foggy': 4,
        'drizzle': 5,
        'rain': 6,
        'snow': 7,
        'heavyrain': 8,
        'storm': 9,
        'typhoon': 10
    }
    
    def __init__(self):
        self.logger = logger
        
    def compare_weather(self, 
                       old_weather: Dict[str, Any], 
                       new_weather: Dict[str, Any],
                       user_preferences: Optional[Dict[str, Any]] = None) -> List[WeatherChange]:
        """
        이전 날씨와 새로운 날씨를 비교하여 변경사항 반환
        
        Args:
            old_weather: 이전 날씨 정보
            new_weather: 새로운 날씨 정보
            user_preferences: 사용자 알림 설정
            
        Returns:
            변경사항 리스트
        """
        changes = []
        
        # 사용자 설정이 없으면 기본값 사용
        if not user_preferences:
            user_preferences = {
                'min_temperature_change': self.TEMP_CHANGE_THRESHOLD,
                'rain_probability_threshold': self.RAIN_CHANGE_THRESHOLD
            }
        
        # 날짜별로 비교
        for date_str, old_data in old_weather.items():
            if date_str not in new_weather:
                continue
                
            new_data = new_weather[date_str]
            weather_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # 온도 변화 체크
            temp_changes = self._check_temperature_changes(
                old_data, new_data, weather_date, 
                user_preferences['min_temperature_change']
            )
            changes.extend(temp_changes)
            
            # 강수 확률 변화 체크
            rain_changes = self._check_rain_probability_changes(
                old_data, new_data, weather_date,
                user_preferences['rain_probability_threshold']
            )
            changes.extend(rain_changes)
            
            # 날씨 상태 변화 체크
            condition_changes = self._check_weather_condition_changes(
                old_data, new_data, weather_date
            )
            changes.extend(condition_changes)
            
            # 특수 날씨 경보 체크
            alert_changes = self._check_weather_alerts(
                old_data, new_data, weather_date
            )
            changes.extend(alert_changes)
        
        return changes
    
    def _check_temperature_changes(self, 
                                  old_data: Dict, 
                                  new_data: Dict, 
                                  weather_date: date,
                                  threshold: float) -> List[WeatherChange]:
        """온도 변화 체크"""
        changes = []
        
        # 최고 온도 변화
        if 'max_temp' in old_data and 'max_temp' in new_data:
            old_max = float(old_data['max_temp'])
            new_max = float(new_data['max_temp'])
            diff = abs(new_max - old_max)
            
            if diff >= threshold:
                severity = 'warning' if diff >= threshold * 1.5 else 'info'
                changes.append(WeatherChange(
                    date=weather_date,
                    field='max_temperature',
                    old_value=old_max,
                    new_value=new_max,
                    change_type='temperature',
                    severity=severity,
                    description=f"최고 기온이 {old_max}°C에서 {new_max}°C로 {diff:.1f}도 변경되었습니다."
                ))
        
        # 최저 온도 변화
        if 'min_temp' in old_data and 'min_temp' in new_data:
            old_min = float(old_data['min_temp'])
            new_min = float(new_data['min_temp'])
            diff = abs(new_min - old_min)
            
            if diff >= threshold:
                severity = 'warning' if diff >= threshold * 1.5 else 'info'
                changes.append(WeatherChange(
                    date=weather_date,
                    field='min_temperature',
                    old_value=old_min,
                    new_value=new_min,
                    change_type='temperature',
                    severity=severity,
                    description=f"최저 기온이 {old_min}°C에서 {new_min}°C로 {diff:.1f}도 변경되었습니다."
                ))
        
        return changes
    
    def _check_rain_probability_changes(self,
                                       old_data: Dict,
                                       new_data: Dict,
                                       weather_date: date,
                                       threshold: int) -> List[WeatherChange]:
        """강수 확률 변화 체크"""
        changes = []
        
        if 'rain_probability' in old_data and 'rain_probability' in new_data:
            old_prob = int(old_data.get('rain_probability', 0))
            new_prob = int(new_data.get('rain_probability', 0))
            
            # 강수 확률이 임계값을 넘는 경우
            if old_prob < threshold <= new_prob:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='rain_probability',
                    old_value=old_prob,
                    new_value=new_prob,
                    change_type='rain',
                    severity='warning',
                    description=f"강수 확률이 {old_prob}%에서 {new_prob}%로 증가했습니다. 우산을 준비하세요!"
                ))
            elif old_prob >= threshold > new_prob:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='rain_probability',
                    old_value=old_prob,
                    new_value=new_prob,
                    change_type='rain',
                    severity='info',
                    description=f"강수 확률이 {old_prob}%에서 {new_prob}%로 감소했습니다."
                ))
        
        return changes
    
    def _check_weather_condition_changes(self,
                                        old_data: Dict,
                                        new_data: Dict,
                                        weather_date: date) -> List[WeatherChange]:
        """날씨 상태 변화 체크"""
        changes = []
        
        old_condition = old_data.get('weather_condition', '').lower()
        new_condition = new_data.get('weather_condition', '').lower()
        
        if old_condition != new_condition:
            old_severity = self.WEATHER_SEVERITY.get(old_condition, 0)
            new_severity = self.WEATHER_SEVERITY.get(new_condition, 0)
            
            # 날씨가 나빠진 경우
            if new_severity > old_severity:
                severity = 'critical' if new_severity >= 8 else 'warning'
                changes.append(WeatherChange(
                    date=weather_date,
                    field='weather_condition',
                    old_value=old_condition,
                    new_value=new_condition,
                    change_type='condition',
                    severity=severity,
                    description=f"날씨가 {self._translate_condition(old_condition)}에서 "
                               f"{self._translate_condition(new_condition)}(으)로 변경되었습니다."
                ))
            # 날씨가 좋아진 경우
            elif new_severity < old_severity:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='weather_condition',
                    old_value=old_condition,
                    new_value=new_condition,
                    change_type='condition',
                    severity='info',
                    description=f"날씨가 {self._translate_condition(old_condition)}에서 "
                               f"{self._translate_condition(new_condition)}(으)로 개선되었습니다."
                ))
        
        return changes
    
    def _check_weather_alerts(self,
                             old_data: Dict,
                             new_data: Dict,
                             weather_date: date) -> List[WeatherChange]:
        """특수 날씨 경보 체크"""
        changes = []
        
        # 폭염 경보
        if 'max_temp' in new_data and float(new_data['max_temp']) >= 35:
            if 'max_temp' not in old_data or float(old_data['max_temp']) < 35:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='heat_warning',
                    old_value=False,
                    new_value=True,
                    change_type='alert',
                    severity='critical',
                    description=f"폭염 경보: 최고 기온이 {new_data['max_temp']}°C로 예상됩니다."
                ))
        
        # 한파 경보
        if 'min_temp' in new_data and float(new_data['min_temp']) <= -10:
            if 'min_temp' not in old_data or float(old_data['min_temp']) > -10:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='cold_warning',
                    old_value=False,
                    new_value=True,
                    change_type='alert',
                    severity='critical',
                    description=f"한파 경보: 최저 기온이 {new_data['min_temp']}°C로 예상됩니다."
                ))
        
        # 강풍 경보
        if 'wind_speed' in new_data and float(new_data.get('wind_speed', 0)) >= 20:
            if 'wind_speed' not in old_data or float(old_data.get('wind_speed', 0)) < 20:
                changes.append(WeatherChange(
                    date=weather_date,
                    field='wind_warning',
                    old_value=False,
                    new_value=True,
                    change_type='alert',
                    severity='warning',
                    description=f"강풍 주의보: 풍속이 {new_data['wind_speed']}m/s로 예상됩니다."
                ))
        
        return changes
    
    def _translate_condition(self, condition: str) -> str:
        """영문 날씨 상태를 한글로 변환"""
        translations = {
            'sunny': '맑음',
            'cloudy': '구름많음',
            'overcast': '흐림',
            'foggy': '안개',
            'drizzle': '이슬비',
            'rain': '비',
            'snow': '눈',
            'heavyrain': '폭우',
            'storm': '폭풍',
            'typhoon': '태풍'
        }
        return translations.get(condition, condition)
    
    def get_notification_message(self, changes: List[WeatherChange], plan_info: Dict) -> Dict[str, str]:
        """변경사항을 기반으로 알림 메시지 생성"""
        if not changes:
            return {}
        
        # 심각도별로 그룹화
        critical_changes = [c for c in changes if c.severity == 'critical']
        warning_changes = [c for c in changes if c.severity == 'warning']
        info_changes = [c for c in changes if c.severity == 'info']
        
        # 제목 생성
        if critical_changes:
            subject = f"⚠️ 긴급: {plan_info['destination']} 여행 날씨 경보"
        elif warning_changes:
            subject = f"📢 주의: {plan_info['destination']} 여행 날씨 변경"
        else:
            subject = f"ℹ️ 알림: {plan_info['destination']} 여행 날씨 업데이트"
        
        # 본문 생성
        body_parts = [
            f"안녕하세요, {plan_info['user_name']}님!",
            f"",
            f"{plan_info['start_date']} ~ {plan_info['end_date']} 일정의",
            f"{plan_info['destination']} 여행 날씨에 변화가 있습니다.",
            f"",
            "📋 주요 변경사항:"
        ]
        
        # 날짜별로 정렬하여 표시
        changes_by_date = {}
        for change in changes:
            if change.date not in changes_by_date:
                changes_by_date[change.date] = []
            changes_by_date[change.date].append(change)
        
        for change_date in sorted(changes_by_date.keys()):
            body_parts.append(f"\n📅 {change_date.strftime('%m월 %d일')}:")
            for change in changes_by_date[change_date]:
                emoji = "🔴" if change.severity == 'critical' else "🟡" if change.severity == 'warning' else "🔵"
                body_parts.append(f"  {emoji} {change.description}")
        
        body_parts.extend([
            "",
            "🌈 즐거운 여행 되세요!",
            "",
            "---",
            "이 알림은 Weather Flick의 자동 날씨 모니터링 서비스입니다.",
            "알림 설정은 마이페이지에서 변경하실 수 있습니다."
        ])
        
        return {
            'subject': subject,
            'body': '\n'.join(body_parts),
            'html_body': self._generate_html_body(body_parts, changes_by_date)
        }
    
    def _generate_html_body(self, body_parts: List[str], changes_by_date: Dict) -> str:
        """HTML 형식의 이메일 본문 생성"""
        # 간단한 HTML 템플릿 (실제로는 더 세련된 템플릿 사용 가능)
        html_parts = [
            "<html><body style='font-family: Arial, sans-serif; line-height: 1.6;'>",
            "<div style='max-width: 600px; margin: 0 auto; padding: 20px;'>"
        ]
        
        for part in body_parts:
            if part.startswith("📋"):
                html_parts.append(f"<h2>{part}</h2>")
            elif part.startswith("📅"):
                html_parts.append(f"<h3 style='color: #2c5aa0;'>{part}</h3>")
            elif "🔴" in part:
                html_parts.append(f"<p style='color: #d32f2f; margin-left: 20px;'>{part}</p>")
            elif "🟡" in part:
                html_parts.append(f"<p style='color: #f57c00; margin-left: 20px;'>{part}</p>")
            elif "🔵" in part:
                html_parts.append(f"<p style='color: #1976d2; margin-left: 20px;'>{part}</p>")
            elif part == "":
                html_parts.append("<br>")
            elif part.startswith("---"):
                html_parts.append("<hr style='margin: 20px 0;'>")
            else:
                html_parts.append(f"<p>{part}</p>")
        
        html_parts.extend([
            "</div>",
            "</body></html>"
        ])
        
        return ''.join(html_parts)