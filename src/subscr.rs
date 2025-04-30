//! Subscription.

use std::{os::raw::c_void, sync::Arc};
use std::ptr;

use crate::Context;
use crate::{chkerr, connection::Conn, Connection, DpiSubscr, OdpiStr, Result};
use odpic_sys::{
    dpiConn_subscribe, dpiSubscr, dpiSubscrCreateParams, dpiSubscrMessage, dpiSubscrNamespace,
    dpiSubscrProtocol, dpiSubscrQOS, dpiSubscr_addRef, dpiSubscr_prepareStmt, dpiSubscr_release,
    DPI_OPCODE_ALL_OPS, DPI_SUBSCR_NAMESPACE_AQ, DPI_SUBSCR_NAMESPACE_DBCHANGE,
    DPI_SUBSCR_PROTO_CALLBACK, DPI_SUBSCR_PROTO_HTTP, DPI_SUBSCR_PROTO_MAIL,
    DPI_SUBSCR_PROTO_PLSQL, DPI_SUBSCR_QOS_BEST_EFFORT, DPI_SUBSCR_QOS_DEREG_NFY,
    DPI_SUBSCR_QOS_QUERY, DPI_SUBSCR_QOS_RELIABLE, DPI_SUBSCR_QOS_ROWIDS, DPI_SUCCESS,
};

pub enum SubscrNamespace {
    Aq,
    DbChange,
}

impl SubscrNamespace {
    pub(crate) fn to_dpi(self) -> dpiSubscrNamespace {
        match self {
            SubscrNamespace::Aq => DPI_SUBSCR_NAMESPACE_AQ,
            SubscrNamespace::DbChange => DPI_SUBSCR_NAMESPACE_DBCHANGE,
        }
    }
}

pub enum SubscrProtocol {
    Callback,
    Http,
    Mail,
    PlSql,
}

impl SubscrProtocol {
    pub(crate) fn to_dpi(self) -> dpiSubscrProtocol {
        match self {
            SubscrProtocol::Callback => DPI_SUBSCR_PROTO_CALLBACK,
            SubscrProtocol::Http => DPI_SUBSCR_PROTO_HTTP,
            SubscrProtocol::Mail => DPI_SUBSCR_PROTO_MAIL,
            SubscrProtocol::PlSql => DPI_SUBSCR_PROTO_PLSQL,
        }
    }
}

pub enum SubscrQos {
    BestEffort,
    DeregNfy,
    Query,
    Reliable,
    Rowids,
}

impl SubscrQos {
    pub(crate) fn to_dpi(self) -> dpiSubscrQOS {
        match self {
            SubscrQos::BestEffort => DPI_SUBSCR_QOS_BEST_EFFORT,
            SubscrQos::DeregNfy => DPI_SUBSCR_QOS_DEREG_NFY,
            SubscrQos::Query => DPI_SUBSCR_QOS_QUERY,
            SubscrQos::Reliable => DPI_SUBSCR_QOS_RELIABLE,
            SubscrQos::Rowids => DPI_SUBSCR_QOS_ROWIDS,
        }
    }
}

pub struct SubscrCreateParams {
    pub suscr_namespace: Option<SubscrNamespace>,
    pub protocol: Option<SubscrProtocol>,
    pub qos: Option<SubscrQos>,
    pub operations: Option<u32>,
    pub port_number: Option<u32>,
    pub timeout: Option<u32>,
    pub name: Option<String>,
    pub callback: Option<HandlerWrapper>,
    pub recipient_name: Option<String>,
    pub ip_address: Option<String>,
}

impl SubscrCreateParams {
    pub(crate) fn to_dpi(self) -> dpiSubscrCreateParams {
        let name = OdpiStr::new(self.name.unwrap());
        let recipient_name = OdpiStr::new(self.recipient_name.unwrap());
        let ip_address = OdpiStr::new(self.ip_address.unwrap());
        let wrapper = self
            .callback
            .map(|cb| Box::into_raw(Box::new(cb)) as *mut c_void);

        dpiSubscrCreateParams {
            subscrNamespace: self.suscr_namespace.unwrap().to_dpi(),
            protocol: self.protocol.unwrap().to_dpi(),
            qos: self.qos.unwrap().to_dpi(),
            operations: DPI_OPCODE_ALL_OPS,
            portNumber: 0,
            timeout: self.timeout.unwrap(),
            name: name.ptr,
            nameLength: name.len,
            callback: Some(Self::notification_callback),
            callbackContext: wrapper.unwrap(),
            recipientName: recipient_name.ptr,
            recipientNameLength: recipient_name.len,
            ipAddress: ip_address.ptr,
            ipAddressLength: ip_address.len,
            groupingClass: 0,
            groupingValue: 0,
            groupingType: 0,
            outRegId: 0,
            clientInitiated: 0,
        }
    }

    extern "C" fn notification_callback(context: *mut c_void, message: *mut dpiSubscrMessage) {
        unsafe {
            let wrapper_ptr = context as *mut HandlerWrapper;
            let handler = &(*wrapper_ptr).0;
            let msg = NotificationMessage { inner: *message };
            handler(msg);
        }
    }
}

pub struct NotificationMessage {
    pub inner: dpiSubscrMessage,
}

pub struct Subscr {
    pub conn: Conn,
    pub handle: DpiSubscr,
}

pub struct HandlerWrapper(pub Box<dyn Fn(NotificationMessage)>);

impl Connection {
    pub fn subscribe(&self, subscr_create_params: SubscrCreateParams) -> Result<Subscr> {
        let mut params: dpiSubscrCreateParams = unsafe { std::mem::zeroed() };
        params = subscr_create_params.to_dpi();
        let mut subscr_raw = ptr::null_mut();

        chkerr!(
            self.ctxt(),
            dpiConn_subscribe(self.handle(), &mut params, &mut subscr_raw)
        );

        let subscr = DpiSubscr::new(subscr_raw);

        Ok(Subscr { handle: subscr, conn: Arc::clone(&self.conn) })
    }
}

impl Subscr {
    pub(crate) fn ctxt(&self) -> &Context {
        self.conn.ctxt()
    }

    pub(crate) fn handle(&self) -> *mut dpiSubscr {
        self.handle.raw
    }
    
    pub fn add_ref(&self) -> Result<()> {
        chkerr!(self.ctxt(), dpiSubscr_addRef(self.handle()));

        Ok(())
    }

    pub fn prepare_stmt(&self, sql: String) -> Result<()> {
        let sql = OdpiStr::new(sql);
        let stmt = ptr::null_mut();

        chkerr!(
            self.ctxt(),
            dpiSubscr_prepareStmt(self.handle(), sql.ptr, sql.len, stmt)
        );

        Ok(())
    }

    pub fn release(&self, conn: &Connection) -> Result<()> {
        chkerr!(conn.ctxt(), dpiSubscr_release(self.handle()));

        Ok(())
    }
}
