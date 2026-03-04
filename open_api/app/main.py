from fastapi import FastAPI, Depends, HTTPException, Query, Request, Header
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.orm import Session
from typing import Optional, List
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta

from generated.models.schemas import (
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    ProductListResponse,
    ProductStatus,
    ErrorResponse,
    OrderCreate,
    OrderUpdate,
    OrderResponse,
    OrderItemResponse,
    OrderStatus,
    DiscountType
)

from app.database import (
    get_db, init_db, ProductDB,
    OrderDB, OrderItemDB, PromoCodeDB, UserOperationDB
)
from app.exceptions import (
    APIException,
    ProductNotFoundException,
    ProductInactiveException,
    OrderLimitExceededException,
    OrderHasActiveException,
    InsufficientStockException,
    PromoCodeInvalidException,
    PromoCodeMinAmountException,
    OrderOwnershipViolationException,
    InvalidStateTransitionException,
    OrderNotFoundException
)
from app.middleware import LoggingMiddleware

app = FastAPI(
    title="Marketplace Product API",
    description="API для управления товарами в маркетплейсе",
    version="1.0.0"
)

app.add_middleware(LoggingMiddleware)


@app.exception_handler(APIException)
async def api_exception_handler(request: Request, exc: APIException):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error_code=exc.error_code,
            message=exc.message,
            details=exc.details
        ).model_dump()
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            error_code="VALIDATION_ERROR",
            message="Ошибка валидации входных данных",
            details={"errors": exc.errors()}
        ).model_dump()
    )


@app.on_event("startup")
def startup_event():
    pass


def db_to_response(db_product: ProductDB) -> ProductResponse:
    created_at = db_product.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    
    updated_at = db_product.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    
    return ProductResponse(
        id=UUID(db_product.id),
        name=db_product.name,
        description=db_product.description,
        price=db_product.price,
        category=db_product.category,
        stock=db_product.stock,
        status=db_product.status,
        created_at=created_at,
        updated_at=updated_at
    )


@app.post("/products", response_model=ProductResponse, status_code=201, tags=["Products"])
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    db_product = ProductDB(
        id=str(uuid4()),
        name=product.name,
        description=product.description,
        price=product.price,
        category=product.category,
        stock=product.stock,
        status=ProductStatus.ACTIVE,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_to_response(db_product)


@app.get("/products/{id}", response_model=ProductResponse, tags=["Products"])
def get_product(id: UUID, db: Session = Depends(get_db)):
    db_product = db.query(ProductDB).filter(ProductDB.id == str(id)).first()
    if not db_product:
        raise ProductNotFoundException(details={"id": str(id)})
    return db_to_response(db_product)


@app.get("/products", response_model=ProductListResponse, tags=["Products"])
def get_products(
    page: int = Query(0, ge=0, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    status: Optional[ProductStatus] = Query(None, description="Фильтр по статусу"),
    category: Optional[str] = Query(None, description="Фильтр по категории"),
    db: Session = Depends(get_db)
):
    query = db.query(ProductDB)
    
    if status:
        query = query.filter(ProductDB.status == status)
    if category:
        query = query.filter(ProductDB.category == category)
    
    total_elements = query.count()
    
    products = query.offset(page * size).limit(size).all()
    
    total_pages = (total_elements + size - 1) // size if total_elements > 0 else 0
    
    return ProductListResponse(
        content=[db_to_response(p) for p in products],
        totalElements=total_elements,
        page=page,
        size=size,
        totalPages=total_pages
    )


@app.put("/products/{id}", response_model=ProductResponse, tags=["Products"])
def update_product(id: UUID, product: ProductUpdate, db: Session = Depends(get_db)):
    db_product = db.query(ProductDB).filter(ProductDB.id == str(id)).first()
    if not db_product:
        raise ProductNotFoundException(details={"id": str(id)})
    
    update_data = product.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_product, field, value)
    
    db_product.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(db_product)
    return db_to_response(db_product)


@app.delete("/products/{id}", status_code=204, tags=["Products"])
def delete_product(id: UUID, db: Session = Depends(get_db)):
    db_product = db.query(ProductDB).filter(ProductDB.id == str(id)).first()
    if not db_product:
        raise ProductNotFoundException(details={"id": str(id)})
    
    db_product.status = ProductStatus.ARCHIVED
    db_product.updated_at = datetime.now(timezone.utc)
    db.commit()
    return None


@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "message": "Marketplace Product API is running",
        "version": "1.0.0"
    }


def get_current_user_id(x_user_id: Optional[str] = Header(None)) -> str:
    if x_user_id:
        return x_user_id
    return "test-user-id"


def check_order_limit(user_id: str, operation_type: str, db: Session, limit_minutes: int = 1):
    last_op = db.query(UserOperationDB).filter(
        UserOperationDB.user_id == user_id,
        UserOperationDB.operation_type == operation_type
    ).order_by(UserOperationDB.created_at.desc()).first()

    if last_op:
        last_time = last_op.created_at
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone.utc)
        
        if datetime.now(timezone.utc) - last_time < timedelta(minutes=limit_minutes):
            raise OrderLimitExceededException()


def db_order_to_response(db_order: OrderDB) -> OrderResponse:
    items = []
    for item in db_order.items:
        items.append(OrderItemResponse(
            id=UUID(item.id),
            product_id=UUID(item.product_id),
            quantity=item.quantity,
            price_at_order=item.price_at_order
        ))
    
    created_at = db_order.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
        
    updated_at = db_order.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    return OrderResponse(
        id=UUID(db_order.id),
        user_id=db_order.user_id,
        status=db_order.status,
        items=items,
        total_amount=db_order.total_amount,
        discount_amount=db_order.discount_amount,
        promo_code_id=UUID(db_order.promo_code_id) if db_order.promo_code_id else None,
        created_at=created_at,
        updated_at=updated_at
    )


@app.post("/orders", response_model=OrderResponse, status_code=201, tags=["Orders"])
def create_order(
    order_data: OrderCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    check_order_limit(user_id, "CREATE_ORDER", db)

    active_order = db.query(OrderDB).filter(
        OrderDB.user_id == user_id,
        OrderDB.status.in_([OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING])
    ).first()
    if active_order:
        raise OrderHasActiveException()

    product_ids = [str(item.product_id) for item in order_data.items]
    products = db.query(ProductDB).filter(ProductDB.id.in_(product_ids)).all()
    products_map = {p.id: p for p in products}

    insufficient_stock = []
    
    for item in order_data.items:
        pid = str(item.product_id)
        if pid not in products_map:
            raise ProductNotFoundException(details={"product_id": pid})
        
        product = products_map[pid]
        if product.status != ProductStatus.ACTIVE:
            raise ProductInactiveException(details={"product_id": pid})
            
        if product.stock < item.quantity:
            insufficient_stock.append({
                "product_id": pid,
                "requested": item.quantity,
                "available": product.stock
            })
    
    if insufficient_stock:
        raise InsufficientStockException(details={"items": insufficient_stock})

    try:
        order_id = str(uuid4())
        total_amount = 0.0
        db_items = []
        
        for item in order_data.items:
            pid = str(item.product_id)
            product = products_map[pid]
            
            product.stock -= item.quantity
            price = product.price
            total_amount += price * item.quantity
            
            db_item = OrderItemDB(
                id=str(uuid4()),
                order_id=order_id,
                product_id=pid,
                quantity=item.quantity,
                price_at_order=price
            )
            db_items.append(db_item)

        discount_amount = 0.0
        promo_code_id = None
        
        if order_data.promo_code:
            promo = db.query(PromoCodeDB).filter(PromoCodeDB.code == order_data.promo_code).first()
            if not promo:
                raise PromoCodeInvalidException(details={"code": order_data.promo_code})
            
            now = datetime.now(timezone.utc)
            valid_from = promo.valid_from.replace(tzinfo=timezone.utc) if promo.valid_from.tzinfo is None else promo.valid_from
            valid_until = promo.valid_until.replace(tzinfo=timezone.utc) if promo.valid_until.tzinfo is None else promo.valid_until

            if (not promo.active or
                promo.current_uses >= promo.max_uses or
                not (valid_from <= now <= valid_until)):
                raise PromoCodeInvalidException(details={"code": order_data.promo_code})
            
            if total_amount < promo.min_order_amount:
                raise PromoCodeMinAmountException(details={
                    "min_amount": promo.min_order_amount,
                    "total_amount": total_amount
                })
            
            if promo.discount_type == DiscountType.PERCENTAGE:
                discount = total_amount * promo.discount_value / 100
                max_discount = total_amount * 0.7
                discount = min(discount, max_discount)
            else:
                discount = min(promo.discount_value, total_amount)
            
            discount_amount = discount
            total_amount -= discount
            
            promo.current_uses += 1
            promo_code_id = promo.id

        db_order = OrderDB(
            id=order_id,
            user_id=user_id,
            status=OrderStatus.CREATED,
            promo_code_id=promo_code_id,
            total_amount=total_amount,
            discount_amount=discount_amount,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        db.add(db_order)
        for item in db_items:
            db.add(item)
            
        op = UserOperationDB(
            id=str(uuid4()),
            user_id=user_id,
            operation_type="CREATE_ORDER",
            created_at=datetime.now(timezone.utc)
        )
        db.add(op)
        
        db.commit()
        db.refresh(db_order)
        return db_order_to_response(db_order)
        
    except Exception as e:
        db.rollback()
        raise e


@app.put("/orders/{id}", response_model=OrderResponse, tags=["Orders"])
def update_order(
    id: UUID,
    order_data: OrderUpdate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    db_order = db.query(OrderDB).filter(OrderDB.id == str(id)).first()
    if not db_order:
        raise OrderNotFoundException(details={"id": str(id)})

    if db_order.user_id != user_id:
        raise OrderOwnershipViolationException()

    if db_order.status != OrderStatus.CREATED:
        raise InvalidStateTransitionException(details={"current_status": db_order.status})

    check_order_limit(user_id, "UPDATE_ORDER", db)

    try:
        for item in db_order.items:
            product = db.query(ProductDB).filter(ProductDB.id == item.product_id).first()
            if product:
                product.stock += item.quantity
        
        db.query(OrderItemDB).filter(OrderItemDB.order_id == str(id)).delete()
        
        product_ids = [str(item.product_id) for item in order_data.items]
        products = db.query(ProductDB).filter(ProductDB.id.in_(product_ids)).all()
        products_map = {p.id: p for p in products}
        
        insufficient_stock = []
        total_amount = 0.0
        db_items = []
        
        for item in order_data.items:
            pid = str(item.product_id)
            if pid not in products_map:
                raise ProductNotFoundException(details={"product_id": pid})
            
            product = products_map[pid]
            if product.status != ProductStatus.ACTIVE:
                raise ProductInactiveException(details={"product_id": pid})
                
            if product.stock < item.quantity:
                insufficient_stock.append({
                    "product_id": pid,
                    "requested": item.quantity,
                    "available": product.stock
                })
            
            product.stock -= item.quantity
            price = product.price
            total_amount += price * item.quantity
            
            db_item = OrderItemDB(
                id=str(uuid4()),
                order_id=str(id),
                product_id=pid,
                quantity=item.quantity,
                price_at_order=price
            )
            db_items.append(db_item)
            
        if insufficient_stock:
            raise InsufficientStockException(details={"items": insufficient_stock})

        discount_amount = 0.0
        
        if db_order.promo_code_id:
            promo = db.query(PromoCodeDB).filter(PromoCodeDB.id == db_order.promo_code_id).first()
            if promo:
                now = datetime.now(timezone.utc)
                valid_from = promo.valid_from.replace(tzinfo=timezone.utc) if promo.valid_from.tzinfo is None else promo.valid_from
                valid_until = promo.valid_until.replace(tzinfo=timezone.utc) if promo.valid_until.tzinfo is None else promo.valid_until
                
                is_valid = True
                if not promo.active or not (valid_from <= now <= valid_until):
                    is_valid = False
                
                if total_amount < promo.min_order_amount:
                    is_valid = False
                
                if is_valid:
                    if promo.discount_type == DiscountType.PERCENTAGE:
                        discount = total_amount * promo.discount_value / 100
                        max_discount = total_amount * 0.7
                        discount = min(discount, max_discount)
                    else:
                        discount = min(promo.discount_value, total_amount)
                    
                    discount_amount = discount
                    total_amount -= discount
                else:
                    db_order.promo_code_id = None
                    promo.current_uses -= 1
        
        db_order.total_amount = total_amount
        db_order.discount_amount = discount_amount
        db_order.updated_at = datetime.now(timezone.utc)
        
        for item in db_items:
            db.add(item)
            
        op = UserOperationDB(
            id=str(uuid4()),
            user_id=user_id,
            operation_type="UPDATE_ORDER",
            created_at=datetime.now(timezone.utc)
        )
        db.add(op)
        
        db.commit()
        db.refresh(db_order)
        return db_order_to_response(db_order)
        
    except Exception as e:
        db.rollback()
        raise e


@app.post("/orders/{id}/cancel", response_model=OrderResponse, tags=["Orders"])
def cancel_order(
    id: UUID,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    db_order = db.query(OrderDB).filter(OrderDB.id == str(id)).first()
    if not db_order:
        raise OrderNotFoundException(details={"id": str(id)})

    if db_order.user_id != user_id:
        raise OrderOwnershipViolationException()

    if db_order.status not in [OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING]:
        raise InvalidStateTransitionException(details={"current_status": db_order.status})

    try:
        for item in db_order.items:
            product = db.query(ProductDB).filter(ProductDB.id == item.product_id).first()
            if product:
                product.stock += item.quantity
        
        if db_order.promo_code_id:
            promo = db.query(PromoCodeDB).filter(PromoCodeDB.id == db_order.promo_code_id).first()
            if promo:
                promo.current_uses -= 1
        
        db_order.status = OrderStatus.CANCELED
        db_order.updated_at = datetime.now(timezone.utc)
        
        db.commit()
        db.refresh(db_order)
        return db_order_to_response(db_order)
        
    except Exception as e:
        db.rollback()
        raise e