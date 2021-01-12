//package cn.mycat.vertx.xa;
//
//import io.reactivex.Maybe;
//import io.reactivex.MaybeObserver;
//import io.reactivex.annotations.NonNull;
//import io.reactivex.disposables.Disposable;
//
//public class Main {
//    public static void main(String[] args) {
//        Maybe<Integer> integerMaybe = Maybe.just(1)
//
//                .map(v -> v + 1).toObservable().
//                .filter(v -> v == 1)
//        integerMaybe.subscribe(new MaybeObserver<Integer>() {
//            @Override
//            public void onSubscribe(@NonNull Disposable d) {
//
//            }
//
//            @Override
//            public void onSuccess(@NonNull Integer integer) {
//
//            }
//
//            @Override
//            public void onError(@NonNull Throwable e) {
//
//            }
//
//            @Override
//            public void onComplete() {
//
//            }
//        });
//    }
//}
