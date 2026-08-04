package io.bordo.ddpclient.ddpclient

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map


sealed class MethodState<out T> {
    data class Success<T>(val response: T?) : MethodState<T>()
    data class Error(
        val error: ResponseError? = null
    ) : MethodState<Nothing>()
    data class Exception(val exception: Throwable? = null): MethodState<Nothing>()
    data object Loading : MethodState<Nothing>()
    data object Idle : MethodState<Nothing>()
    data object Updated : MethodState<Nothing>()
}
fun Incoming.Error.toMethodStateException() = MethodState.Exception(
    exception = Exception(reason)
)
fun Incoming.Exception.toMethodStateException() = MethodState.Exception(exception = exception)

inline fun <T, R> MethodState<T>.transform(
    crossinline transform: (value: T?) -> R
): MethodState<R> = when (this) {
    is MethodState.Success -> {
        MethodState.Success(transform(content))
    }
    is MethodState.Error -> MethodState.Error(error)
    is MethodState.Exception -> MethodState.Exception(exception)
    MethodState.Idle -> MethodState.Idle
    MethodState.Loading -> MethodState.Loading
    MethodState.Updated -> MethodState.Updated
}

inline fun <T, R> MethodState<T>.transformNotNull(
    crossinline transform: (value: T) -> R
): MethodState<R> = when (this) {
    is MethodState.Success -> {
        content?.let {
            MethodState.Success(transform(it))
        } ?: error("Cannot transform. Consider using transform instead")
    }
    is MethodState.Error -> MethodState.Error(error)
    is MethodState.Exception -> MethodState.Exception(exception)
    MethodState.Idle -> MethodState.Idle
    MethodState.Loading -> MethodState.Loading
    MethodState.Updated -> MethodState.Updated
}


inline fun <T, R> Flow<MethodState<T>>.transform(
    crossinline transform: (value: T?) -> R
): Flow<MethodState<R>> = map { it.transform(transform) }


inline fun <T, R> Flow<MethodState<T>>.transformNotNull(
    crossinline transform: (value: T) -> R
): Flow<MethodState<R>> = map { it.transformNotNull(transform) }

@OptIn(ExperimentalCoroutinesApi::class)
inline fun <T, R> Flow<MethodState<T>>.flatMapLatestMethodState(
    crossinline transform: (value: T) -> Flow<MethodState<R>>
): Flow<MethodState<R>> = flatMapLatest { state ->
    when (state) {
        is MethodState.Success -> state.content?.let { transform(it) } ?: error("Cannot flatmap with null response. use transform instead")
        is MethodState.Error -> flowOf(MethodState.Error(state.error))
        is MethodState.Exception -> flowOf(MethodState.Exception(state.exception))
        MethodState.Idle -> flowOf(MethodState.Idle)
        MethodState.Loading -> flowOf(MethodState.Loading)
        MethodState.Updated -> flowOf(MethodState.Updated)
    }
}

val MethodState<*>.succeededWithoutContent
    get() = this is MethodState.Success

val MethodState<*>.succeeded
    get() = succeededWithoutContent && content != null

val MethodState<*>.failed
    get() = this is MethodState.Error || this is MethodState.Exception

val MethodState<*>.loading
    get() = this is MethodState.Loading || this is MethodState.Updated

val MethodState<*>.idle
    get() = this is MethodState.Idle

val MethodState<*>.error
    get() = this as? MethodState.Error

val MethodState<*>.exception
    get() = this as? MethodState.Exception

val <T> MethodState<T>.content: T?
    get() = (this as? MethodState.Success)?.response

