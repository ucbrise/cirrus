#ifndef CIRCULAR_BUFFER_H
#define CIRCULAR_BUFFER_H

#include <algorithm>
#include <cstddef>
#include <cassert>
#include <stdexcept>
#include <iostream>

template <typename T>
class CircularBuffer
{
  public:
    typedef T& reference;
    typedef const T& const_reference;
    typedef T* pointer;
    typedef const T* const_pointer;

    explicit CircularBuffer(uint64_t capacity);
    CircularBuffer(const CircularBuffer<T> &rhs);
    CircularBuffer(CircularBuffer<T>&& rhs);
    ~CircularBuffer() { if (_buffer) delete[] _buffer; }

    CircularBuffer<T>& operator=(CircularBuffer<T> rhs);

    uint64_t size() const;
    uint64_t capacity() const { return _capacity; }

    void add(T item); // we add at the 
    T pop();
    T& front();

    friend void swap(CircularBuffer<T> &a, CircularBuffer<T> &b) {
      std::swap(a._buffer, b._buffer);
      std::swap(a._capacity, b._capacity);
      std::swap(a._front, b._front);
      std::swap(a._full, b._full);
    }

  private:
    pointer _buffer;
    uint64_t _capacity;

    // we add at the tail and we pop from front
    uint64_t _front;
    uint64_t _tail;

    CircularBuffer();
};

template<typename T>
uint64_t CircularBuffer<T>::size() const {
  if (_tail < _front) { // we wrapped around
    return _capacity - _front + _tail;
  } else {
    return _tail - _front;
  }
}

template<typename T>
T CircularBuffer<T>::pop() {
  if (size() == 0) {
    throw std::runtime_error("pop: Empty circular buffer");
  }
  T ret = _buffer[_front];
  _front = (_front + 1) % _capacity;

  return ret;
}

template<typename T>
T& CircularBuffer<T>::front() {
  if (size() == 0) {
    throw std::runtime_error("front(): Empty circular buffer");
  }
  T& ret = _buffer[_front];
  return ret;
}

template<typename T>
CircularBuffer<T>::CircularBuffer()
: _buffer(nullptr), _capacity(0), _front(0), _tail(0)
{
}

template<typename T>
CircularBuffer<T>::CircularBuffer(uint64_t capacity)
  : CircularBuffer() {
  if (capacity < 1) throw std::length_error("Invalid capacity");

  _buffer = new T[capacity];
  _capacity = capacity;
}

template<typename T>
CircularBuffer<T>::CircularBuffer(const CircularBuffer<T> &rhs)
: _buffer(new T[rhs._capacity]), _capacity(rhs._capacity), _front(rhs._front), _tail(rhs._tail)
{
  std::copy(rhs._buffer, rhs._buffer + _capacity, _buffer);
}

template<typename T>
CircularBuffer<T>::CircularBuffer(CircularBuffer<T>&& rhs)
  : CircularBuffer()
{
  swap(*this, rhs);
}

template<typename T>
CircularBuffer<T>& 
CircularBuffer<T>::operator=(CircularBuffer<T> rhs)
{
  swap(*this, rhs);
  return *this;
}

template<typename T>
void 
CircularBuffer<T>::add(T item)
{
  _buffer[_tail++] = item;
  if (_tail == _capacity) {
    _tail = 0;
  }
}

#endif // CIRCULAR_BUFFER_H
