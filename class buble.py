from cryptography.fernet import Fernet
from threading import Lock
import time


class BubbleContainer:
    def __init__(self, key=None):
        self.key = key or Fernet.generate_key()
        self.fernet = Fernet(self.key)
        self._data = {}  # Хранение зашифрованных данных по ключам
        self._timestamps = {}  # Хранение времени добавления данных
        self.lock = Lock()  # Для потокобезопасности

    # Добавление данных с шифрованием и необязательным временем жизни (в секундах)
    def add(self, item, bubble_key, ttl=None):
        with self.lock:
            encrypted_item = self.fernet.encrypt(str(item).encode())
            self._data[bubble_key] = encrypted_item
            if ttl:
                self._timestamps[bubble_key] = time.time() + ttl  # Устанавливаем срок действия данных

    # Поиск данных по ключу и их удаление после просмотра
    def search(self, bubble_key):
        with self.lock:
            self._check_expiration(bubble_key)
            if bubble_key in self._data:
                encrypted_item = self._data[bubble_key]
                item = self.fernet.decrypt(encrypted_item).decode()
                del self._data[bubble_key]  # Удаляем данные после просмотра
                self._timestamps.pop(bubble_key, None)  # Удаляем время жизни
                return item
            else:
                raise KeyError(f"Ключ '{bubble_key}' не найден")

    # Удаление данных по ключу
    def remove(self, bubble_key):
        with self.lock:
            if bubble_key in self._data:
                del self._data[bubble_key]
                self._timestamps.pop(bubble_key, None)
            else:
                raise KeyError(f"Ключ '{bubble_key}' не найден")

    # Слияние нескольких пузырей в один
    def merge(self, keys, new_bubble_key):
        if not keys:
            raise ValueError("Список ключей не может быть пустым")

        merged_data = None
        with self.lock:
            for bubble_key in keys:
                self._check_expiration(bubble_key)
                if bubble_key in self._data:
                    encrypted_item = self._data[bubble_key]
                    item = self.fernet.decrypt(encrypted_item).decode()

                    # Если первый элемент, присваиваем как merged_data
                    if merged_data is None:
                        merged_data = self._parse_type(item)
                    else:
                        merged_data = self._merge_values(merged_data, self._parse_type(item))

                    # Удаляем маленький пузырь
                    del self._data[bubble_key]
                    self._timestamps.pop(bubble_key, None)
                else:
                    raise KeyError(f"Ключ '{bubble_key}' не найден")

            self.add(merged_data, new_bubble_key)
            return merged_data

    # Проверка на срок действия данных
    def _check_expiration(self, bubble_key):
        if bubble_key in self._timestamps:
            if time.time() > self._timestamps[bubble_key]:
                del self._data[bubble_key]
                del self._timestamps[bubble_key]
                raise KeyError(f"Ключ '{bubble_key}' истек")

    # Обработка типов для корректного слияния
    def _parse_type(self, item):
        try:
            return eval(item)  # Попытка преобразовать строку обратно в исходный тип
        except:
            return item  # Если не удается, оставляем как строку

    # Слияние значений в зависимости от их типа
    def _merge_values(self, merged_data, new_data):
        if isinstance(merged_data, str) and isinstance(new_data, str):
            return merged_data + new_data
        elif isinstance(merged_data, (int, float)) and isinstance(new_data, (int, float)):
            return merged_data + new_data
        elif isinstance(merged_data, dict) and isinstance(new_data, dict):
            merged_data.update(new_data)
            return merged_data
        elif isinstance(merged_data, list) and isinstance(new_data, list):
            merged_data.extend(new_data)
            return merged_data
        else:
            return str(merged_data) + str(new_data)

    # Запрет на случайный доступ и извлечение всех ключей
    def size(self):
        with self.lock:
            return len(self._data)

    # Закрываем возможность прямого доступа к данным через __str__ или другие методы
    def __str__(self):
        return "<BubbleContainer: access restricted>"

    # Автоматическое удаление данных по истечении времени
    def _auto_cleanup(self):
        with self.lock:
            current_time = time.time()
            expired_keys = [key for key, expiry in self._timestamps.items() if current_time > expiry]
            for key in expired_keys:
                del self._data[key]
                del self._timestamps[key]


# Пример использования контейнера
if __name__ == "__main__":
    bubble = BubbleContainer()

    # Добавляем данные
    print("Adding 'Hello' to bubble1 with TTL 5 seconds")
    bubble.add("Hello", "bubble1", ttl=5)

    print("Adding ', World!' to bubble2")
    bubble.add(", World!", "bubble2")

    print("Adding number 42 to bubble3")
    bubble.add(42, "bubble3")

    # Поиск и вывод данных
    try:
        print("Retrieved from bubble1:", bubble.search("bubble1"))
    except KeyError as e:
        print(e)

    # Слияние и проверка
    try:
        merged_data = bubble.merge(["bubble2", "bubble3"], "super_bubble")
        print("Merged data:", merged_data)
    except KeyError as e:
        print(e)
    except Exception as e:
        print("Error during merge:", e)

    print("Number of items in container after merge:", bubble.size())

