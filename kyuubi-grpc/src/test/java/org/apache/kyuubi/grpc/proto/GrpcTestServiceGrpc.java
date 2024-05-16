package org.apache.kyuubi.grpc.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.60.1)",
    comments = "Source: test_case.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class GrpcTestServiceGrpc {

  private GrpcTestServiceGrpc() {}

  public static final String SERVICE_NAME = "kyuubi.grpc.GrpcTestService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<TestOpenSessionRequest,
      TestOpenSessionResponse> getTestOpenSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TestOpenSession",
      requestType = TestOpenSessionRequest.class,
      responseType = TestOpenSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<TestOpenSessionRequest,
      TestOpenSessionResponse> getTestOpenSessionMethod() {
    io.grpc.MethodDescriptor<TestOpenSessionRequest, TestOpenSessionResponse> getTestOpenSessionMethod;
    if ((getTestOpenSessionMethod = GrpcTestServiceGrpc.getTestOpenSessionMethod) == null) {
      synchronized (GrpcTestServiceGrpc.class) {
        if ((getTestOpenSessionMethod = GrpcTestServiceGrpc.getTestOpenSessionMethod) == null) {
          GrpcTestServiceGrpc.getTestOpenSessionMethod = getTestOpenSessionMethod =
              io.grpc.MethodDescriptor.<TestOpenSessionRequest, TestOpenSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TestOpenSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  TestOpenSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  TestOpenSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GrpcTestServiceMethodDescriptorSupplier("TestOpenSession"))
              .build();
        }
      }
    }
    return getTestOpenSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<TestAddRequest,
      TestAddResponse> getTestAddMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TestAdd",
      requestType = TestAddRequest.class,
      responseType = TestAddResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<TestAddRequest,
      TestAddResponse> getTestAddMethod() {
    io.grpc.MethodDescriptor<TestAddRequest, TestAddResponse> getTestAddMethod;
    if ((getTestAddMethod = GrpcTestServiceGrpc.getTestAddMethod) == null) {
      synchronized (GrpcTestServiceGrpc.class) {
        if ((getTestAddMethod = GrpcTestServiceGrpc.getTestAddMethod) == null) {
          GrpcTestServiceGrpc.getTestAddMethod = getTestAddMethod =
              io.grpc.MethodDescriptor.<TestAddRequest, TestAddResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TestAdd"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  TestAddRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  TestAddResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GrpcTestServiceMethodDescriptorSupplier("TestAdd"))
              .build();
        }
      }
    }
    return getTestAddMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GrpcTestServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceStub>() {
        @Override
        public GrpcTestServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GrpcTestServiceStub(channel, callOptions);
        }
      };
    return GrpcTestServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GrpcTestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceBlockingStub>() {
        @Override
        public GrpcTestServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GrpcTestServiceBlockingStub(channel, callOptions);
        }
      };
    return GrpcTestServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GrpcTestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GrpcTestServiceFutureStub>() {
        @Override
        public GrpcTestServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GrpcTestServiceFutureStub(channel, callOptions);
        }
      };
    return GrpcTestServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void testOpenSession(TestOpenSessionRequest request,
                                 io.grpc.stub.StreamObserver<TestOpenSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTestOpenSessionMethod(), responseObserver);
    }

    /**
     */
    default void testAdd(TestAddRequest request,
                         io.grpc.stub.StreamObserver<TestAddResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTestAddMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service GrpcTestService.
   */
  public static abstract class GrpcTestServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return GrpcTestServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service GrpcTestService.
   */
  public static final class GrpcTestServiceStub
      extends io.grpc.stub.AbstractAsyncStub<GrpcTestServiceStub> {
    private GrpcTestServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GrpcTestServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GrpcTestServiceStub(channel, callOptions);
    }

    /**
     */
    public void testOpenSession(TestOpenSessionRequest request,
                                io.grpc.stub.StreamObserver<TestOpenSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTestOpenSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void testAdd(TestAddRequest request,
                        io.grpc.stub.StreamObserver<TestAddResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTestAddMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service GrpcTestService.
   */
  public static final class GrpcTestServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<GrpcTestServiceBlockingStub> {
    private GrpcTestServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GrpcTestServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GrpcTestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public TestOpenSessionResponse testOpenSession(TestOpenSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTestOpenSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public TestAddResponse testAdd(TestAddRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTestAddMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service GrpcTestService.
   */
  public static final class GrpcTestServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<GrpcTestServiceFutureStub> {
    private GrpcTestServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GrpcTestServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GrpcTestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<TestOpenSessionResponse> testOpenSession(
        TestOpenSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTestOpenSessionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<TestAddResponse> testAdd(
        TestAddRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTestAddMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_TEST_OPEN_SESSION = 0;
  private static final int METHODID_TEST_ADD = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_TEST_OPEN_SESSION:
          serviceImpl.testOpenSession((TestOpenSessionRequest) request,
              (io.grpc.stub.StreamObserver<TestOpenSessionResponse>) responseObserver);
          break;
        case METHODID_TEST_ADD:
          serviceImpl.testAdd((TestAddRequest) request,
              (io.grpc.stub.StreamObserver<TestAddResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getTestOpenSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              TestOpenSessionRequest,
              TestOpenSessionResponse>(
                service, METHODID_TEST_OPEN_SESSION)))
        .addMethod(
          getTestAddMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              TestAddRequest,
              TestAddResponse>(
                service, METHODID_TEST_ADD)))
        .build();
  }

  private static abstract class GrpcTestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GrpcTestServiceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return TestCase.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GrpcTestService");
    }
  }

  private static final class GrpcTestServiceFileDescriptorSupplier
      extends GrpcTestServiceBaseDescriptorSupplier {
    GrpcTestServiceFileDescriptorSupplier() {}
  }

  private static final class GrpcTestServiceMethodDescriptorSupplier
      extends GrpcTestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GrpcTestServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GrpcTestServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GrpcTestServiceFileDescriptorSupplier())
              .addMethod(getTestOpenSessionMethod())
              .addMethod(getTestAddMethod())
              .build();
        }
      }
    }
    return result;
  }
}
