"""
로깅 유틸리티
구조화된 로깅 설정
"""
import logging
import sys
from datetime import datetime
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """컬러 로그 포매터"""

    # ANSI 색상 코드
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }

    # 이모지
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🔥'
    }

    def format(self, record):
        """로그 포맷팅"""
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.COLORS['RESET'])
        emoji = self.EMOJIS.get(levelname, '📝')
        reset = self.COLORS['RESET']

        # 로그 레벨에 색상 및 이모지 추가
        record.levelname = f"{emoji} {color}{levelname}{reset}"

        return super().format(record)


def setup_logger(
    name: str = "stockmind_api",
    level: int = logging.INFO,
    log_file: str = None
) -> logging.Logger:
    """
    로거 설정

    Args:
        name: 로거 이름
        level: 로그 레벨
        log_file: 로그 파일 경로 (None이면 파일 로깅 안 함)

    Returns:
        logging.Logger: 설정된 로거
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 기존 핸들러 제거 (중복 방지)
    logger.handlers.clear()

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # 컬러 포매터
    console_format = ColoredFormatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)

    logger.addHandler(console_handler)

    # 파일 핸들러 (옵션)
    if log_file:
        # 로그 디렉토리 생성
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)

        # 파일 포매터 (색상 없이)
        file_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)

        logger.addHandler(file_handler)

    return logger


# 전역 로거 인스턴스
api_logger = setup_logger(
    name="stockmind_api",
    level=logging.INFO,
    log_file="api/logs/api.log"
)


def log_request(endpoint: str, method: str, params: dict = None):
    """
    API 요청 로깅

    Args:
        endpoint: 엔드포인트
        method: HTTP 메서드
        params: 요청 파라미터
    """
    api_logger.info(f"📥 {method} {endpoint} | Params: {params or {}}")


def log_response(endpoint: str, status_code: int, duration_ms: float):
    """
    API 응답 로깅

    Args:
        endpoint: 엔드포인트
        status_code: HTTP 상태 코드
        duration_ms: 응답 시간 (밀리초)
    """
    if status_code < 400:
        api_logger.info(f"📤 {endpoint} | Status: {status_code} | {duration_ms:.2f}ms")
    else:
        api_logger.warning(f"⚠️ {endpoint} | Status: {status_code} | {duration_ms:.2f}ms")


def log_error(endpoint: str, error: Exception):
    """
    에러 로깅

    Args:
        endpoint: 엔드포인트
        error: 예외 객체
    """
    api_logger.error(f"❌ {endpoint} | Error: {type(error).__name__}: {str(error)}")


def log_cache_hit(key: str):
    """캐시 히트 로깅"""
    api_logger.debug(f"💾 Cache HIT: {key}")


def log_cache_miss(key: str):
    """캐시 미스 로깅"""
    api_logger.debug(f"🔄 Cache MISS: {key}")


def log_ml_prediction(symbol: str, predicted_price: float, confidence: float):
    """ML 예측 로깅"""
    api_logger.info(
        f"🤖 ML Prediction | {symbol} | "
        f"Price: ${predicted_price:.2f} | "
        f"Confidence: {confidence:.1f}%"
    )


def log_signal_generated(symbol: str, signal: str, confidence: float):
    """매매 신호 생성 로깅"""
    signal_emoji = {
        'BUY': '🟢',
        'SELL': '🔴',
        'HOLD': '🟡'
    }
    emoji = signal_emoji.get(signal, '⚪')

    api_logger.info(
        f"{emoji} Signal Generated | {symbol} | "
        f"Signal: {signal} | "
        f"Confidence: {confidence:.1f}%"
    )
