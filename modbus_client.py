from pymodbus.client import ModbusTcpClient

class FieldLoggerModbus:
    def __init__(self, host: str, port: int = 502, unit_id: int = 255, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.unit_id = unit_id
        self.client = ModbusTcpClient(host, port=port, timeout=timeout)

    def connect(self) -> bool:
        return self.client.connect()

    def close(self):
        self.client.close()

    def read(self) -> dict:
        # analógicos (manual 3..4 => lib 2..3)
        ra = self.client.read_holding_registers(address=3, count=2, device_id=self.unit_id)
        if ra.isError():
            raise RuntimeError(f"Erro analógicos: {ra}")
        a1, a2 = ra.registers

        # digitais (manual 14..15 => lib 13..14)
        rd = self.client.read_holding_registers(address=14, count=2, device_id=self.unit_id)
        if rd.isError():
            raise RuntimeError(f"Erro digitais: {rd}")
        d1, d2 = rd.registers

        return {
            "temperatura_term_raw": a1,
            "corrente_raw": a2,
            "temperatura_term": a1 / 100.0,
            "corrente": a2,
            "botao_start": 1 if d1 > 0 else 0,
            "tampa_descarga": 1 if d2 > 0 else 0,
        }