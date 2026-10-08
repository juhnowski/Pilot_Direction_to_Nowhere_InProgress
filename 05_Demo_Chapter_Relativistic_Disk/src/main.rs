// src/geometry/relativistic_disk.rs

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

/// Константы нашей вселенной
const C: f64 = 300_000.0;       // Скорость света (условные км/с)
const DISK_RADIUS: f64 = 10.0;  // Радиус вращающейся станции (метры)

/// Пакет данных, передаваемый от края диска к центру
#[derive(Debug)]
struct TelemetryPacket {
    edge_local_time: f64,    // Время, зафиксированное часами на краю диска
    computed_perimeter: f64, // Лоренцево сокращение периметра, вычисленное на краю
}

/// Задача для потока на внешнем ободе диска
async fn run_edge_worker(angular_velocity: f64, mut tx: mpsc::Sender<TelemetryPacket>) {
    // 1. Считаем линейную скорость на краю диска: v = w * R
    let v = angular_velocity * DISK_RADIUS;
    
    // Фактор Лоренца (гамма) для замедления времени и сжатия длины
    let lorentz_factor = (1.0 - (v.powi(2) / C.powi(2))).sqrt();

    let mut edge_time = 0.0;
    let initial_perimeter = 2.0 * std::f64::consts::PI * DISK_RADIUS;

    // Цикл симуляции на краю диска
    for _tick in 1..=5 {
        // Имитируем работу: для воркера на краю проходит ровно 1.0 секунда
        sleep(Duration::from_millis(500)).await; 
        edge_time += 1.0;

        // Физический абсурд: из-за Лоренцева сжатия край диска "видит" свой периметр укороченным!
        let contracted_perimeter = initial_perimeter * lorentz_factor;

        let packet = TelemetryPacket {
            edge_local_time: edge_time,
            computed_perimeter: contracted_perimeter,
        };

        // Отправляем пакет сквозь искривленное пространство в центр
        if tx.send(packet).await.is_err() {
            eprintln!("[ОКРАИНА] Канал связи с центром оборвался!");
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    println!("[ЦЕНТР] Станция начинает экстремальный разгон вокруг оси...");
    
    // Скорость вращения (w), при которой скорость на краю близка к скорости света (v = 290 000 км/с)
    let angular_velocity = 29_000.0; 
    
    // Создаем асинхронный канал связи для передачи метрик
    let (tx, mut rx) = mpsc::channel::<TelemetryPacket>(10);

    // Фиксируем стартовое системное время центрального потока
    let start_system_time = std::time::Instant::now();

    // Запускаем независимый асинхронный поток (воркер края диска)
    tokio::spawn(run_edge_worker(angular_velocity, tx));

    // Главный поток (Центр контроля) ожидает данные
    let mut center_time = 0.0;

    while let Some(packet) = rx.recv().await {
        // Сколько физического времени прошло в центре с момента старта
        center_time = start_system_time.elapsed().as_secs_f64() * 2.0; // масштабируем для наглядности

        println!("\n--- НОВАЯ СИНХРОНИЗАЦИЯ ПОТОКОВ ---");
        println!("[ЦЕНТР] Системные часы центра показывают: {:.2} сек.", center_time);
        println!("[ОКРАИНА] Часы на релятивистском краю показывают: {:.2} сек.", packet.edge_local_time);
        println!("[ГЕОМЕТРИЯ] Измеренная длина обода: {:.2} м (Радиус по-прежнему {:.1} м!)", 
                 packet.computed_perimeter, DISK_RADIUS);

        // Детектор релятивистского рассинхрона (Race Condition в физике)
        let time_lag = center_time - packet.edge_local_time;
        println!("[КОНТРОЛЛЕР] Отставание времени воркера от главного потока: {:.2} сек.", time_lag);
    }

    println!("\n[КОНТРОЛЛЕР] Торможение системы. Симуляция завершена. Статус: Триумф!");
}
