"""
히스토리 데이터 API 라우터
댓글 이력 및 통계 조회
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Optional

from api.dependencies import get_db, validate_symbol
from api.schemas.prediction import HistoricalCommentResponse
from api.data.repositories.comment_repo import CommentRepository


router = APIRouter(prefix="/historical", tags=["Historical Data"])


@router.get("/{symbol}/comments", response_model=HistoricalCommentResponse)
def get_historical_comments(
    symbol: str = Depends(validate_symbol),
    start_date: Optional[str] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="최대 조회 개수"),
    db: Session = Depends(get_db)
):
    """
    댓글 이력 조회

    **파라미터:**
    - start_date: 시작 날짜 (없으면 30일 전)
    - end_date: 종료 날짜 (없으면 오늘)
    - limit: 최대 조회 개수 (1 ~ 1000)

    **반환:**
    - 댓글 목록 (시간, 텍스트)
    - 전체 댓글 수
    - 날짜 범위

    **사용처:**
    - 과거 감성 분석
    - 데이터 탐색
    - 백테스팅
    """
    comment_repo = CommentRepository(db)

    # 날짜 파싱
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start = datetime.utcnow() - timedelta(days=30)

    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end = datetime.utcnow()

    # 댓글 조회
    comments = comment_repo.get_comments_by_date_range(symbol, start, end)

    # limit 적용
    limited_comments = comments[:limit]

    # 전체 댓글 수
    total_comments = comment_repo.get_total_comment_count(symbol)

    return HistoricalCommentResponse(
        symbol=symbol,
        total_comments=total_comments,
        date_range={
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d")
        },
        comments=[
            {
                "time": comment.comment_time.isoformat(),
                "text": comment.comment_text[:200],  # 200자로 제한
                "sentiment": "neutral"  # TODO: 실제 감성 분석 추가
            }
            for comment in limited_comments
        ]
    )


@router.get("/{symbol}/statistics")
def get_comment_statistics(
    symbol: str = Depends(validate_symbol),
    days: int = Query(30, ge=1, le=365, description="분석 기간 (일)"),
    db: Session = Depends(get_db)
):
    """
    댓글 통계 조회

    **통계 항목:**
    - 전체 댓글 수
    - 일평균 댓글 수
    - 최대/최소 일별 댓글 수
    - 총 수집 기간

    **파라미터:**
    - days: 분석 기간 (1 ~ 365일)

    **사용처:**
    - 데이터 품질 확인
    - 수집 현황 모니터링
    """
    comment_repo = CommentRepository(db)

    # 전체 댓글 수
    total_comments = comment_repo.get_total_comment_count(symbol)

    # 기간별 댓글 수
    recent_comments = comment_repo.get_comment_count(symbol, hours=days * 24)

    # 일평균
    daily_average = recent_comments / days if days > 0 else 0

    return {
        "symbol": symbol,
        "total_comments": total_comments,
        "recent_comments": recent_comments,
        "analysis_period_days": days,
        "daily_average": round(daily_average, 2),
        "estimated_collection_start": "2025-07-01",  # 실제로는 최초 댓글 날짜
        "last_updated": datetime.utcnow().isoformat()
    }


@router.get("/{symbol}/date-range")
def get_available_date_range(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db)
):
    """
    사용 가능한 데이터 날짜 범위 조회

    **반환:**
    - 최초 댓글 날짜
    - 최신 댓글 날짜
    - 총 수집 일수
    - 데이터 완전성 (일별 댓글 존재 여부)

    **사용처:**
    - API 사용 전 데이터 확인
    - 쿼리 범위 결정
    """
    comment_repo = CommentRepository(db)

    # 전체 댓글 조회 (처음과 끝만)
    all_comments = comment_repo.get_comments_by_date_range(
        symbol,
        datetime(2020, 1, 1),
        datetime.utcnow()
    )

    if not all_comments:
        return {
            "symbol": symbol,
            "first_comment_date": None,
            "last_comment_date": None,
            "total_days": 0,
            "total_comments": 0
        }

    first_date = min(c.comment_time for c in all_comments)
    last_date = max(c.comment_time for c in all_comments)
    total_days = (last_date - first_date).days + 1

    return {
        "symbol": symbol,
        "first_comment_date": first_date.strftime("%Y-%m-%d"),
        "last_comment_date": last_date.strftime("%Y-%m-%d"),
        "total_days": total_days,
        "total_comments": len(all_comments)
    }


@router.get("/all-symbols/summary")
def get_all_symbols_summary(db: Session = Depends(get_db)):
    """
    모든 종목 데이터 요약

    **반환:**
    - 종목별 댓글 수
    - 종목별 최신 업데이트 시각
    - 전체 댓글 수
    - 데이터 수집 상태

    **사용처:**
    - 대시보드
    - 데이터 모니터링
    - API 헬스 체크
    """
    from api.config import settings

    comment_repo = CommentRepository(db)
    summary = []

    total_comments = 0

    for symbol in settings.SUPPORTED_SYMBOLS:
        count = comment_repo.get_total_comment_count(symbol)
        total_comments += count

        # 최근 댓글
        recent = comment_repo.get_recent_comments(symbol, hours=24, limit=1)
        last_updated = recent[0].comment_time.isoformat() if recent else None

        summary.append({
            "symbol": symbol,
            "total_comments": count,
            "last_updated": last_updated,
            "status": "active" if count > 0 else "no_data"
        })

    return {
        "symbols": summary,
        "total_symbols": len(settings.SUPPORTED_SYMBOLS),
        "total_comments_all": total_comments,
        "timestamp": datetime.utcnow().isoformat()
    }
