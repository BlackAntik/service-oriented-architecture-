from typing import Optional, Any, Dict

class APIException(Exception):
    def __init__(
        self,
        error_code: str,
        message: str,
        status_code: int,
        details: Optional[Dict[str, Any]] = None
    ):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code
        self.details = details
        super().__init__(message)

class ProductNotFoundException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="PRODUCT_NOT_FOUND",
            message="Товар не найден по ID",
            status_code=404,
            details=details
        )

class ProductInactiveException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="PRODUCT_INACTIVE",
            message="Попытка заказать неактивный товар",
            status_code=409,
            details=details
        )

class OrderNotFoundException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="ORDER_NOT_FOUND",
            message="Заказ не найден по ID",
            status_code=404,
            details=details
        )

class OrderLimitExceededException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="ORDER_LIMIT_EXCEEDED",
            message="Превышен лимит частоты создания/обновления заказа",
            status_code=429,
            details=details
        )

class OrderHasActiveException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="ORDER_HAS_ACTIVE",
            message="У пользователя уже есть активный заказ",
            status_code=409,
            details=details
        )

class InvalidStateTransitionException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="INVALID_STATE_TRANSITION",
            message="Недопустимый переход состояния заказа",
            status_code=409,
            details=details
        )

class InsufficientStockException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="INSUFFICIENT_STOCK",
            message="Недостаточно товара на складе",
            status_code=409,
            details=details
        )

class PromoCodeInvalidException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="PROMO_CODE_INVALID",
            message="Промокод не найден, истёк, исчерпан или неактивен",
            status_code=422,
            details=details
        )

class PromoCodeMinAmountException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="PROMO_CODE_MIN_AMOUNT",
            message="Сумма заказа ниже минимальной для промокода",
            status_code=422,
            details=details
        )

class OrderOwnershipViolationException(APIException):
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="ORDER_OWNERSHIP_VIOLATION",
            message="Заказ принадлежит другому пользователю",
            status_code=403,
            details=details
        )

class ValidationException(APIException):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            error_code="VALIDATION_ERROR",
            message=message,
            status_code=400,
            details=details
        )