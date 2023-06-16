class Vehiculo:
    def __init__(self, color, anio):
        self.color = color
        self.anio = anio

    def avanza(self):
        print("Avanzando")

    def frenar(self):
        print("Frenando")


class Acuatico(Vehiculo):
    def __init__(self, motor, capacidad, color, anio):
        Vehiculo.__init__(self, color, anio)
        self.motor = motor
        self.capacidad = capacidad

    def remando(self):
        print("Remando con un remo")

    def motor(self):
        print("Motor acuatico encendido")


class Terrestre(Vehiculo):

    def __init__(self, km, licencia, color, anio):
        Vehiculo.__init__(self, color, anio)
        self.km = km
        self.licencia = licencia

    def avanza(self):
        print("Avanzando por carretera")

    def encender(self):
        print("Encendiendo vehiculo terrestre")


class Kayak(Acuatico, Vehiculo):
    def __init__(self, capacidad_kayak, tamanio, color, anio, motor, capacidad):
        Vehiculo.__init__(self, color, anio)
        Acuatico.__init__(self, motor, capacidad, color, anio)
        self.capacidad_kayak = capacidad_kayak
        self.tamanio = tamanio

    def remando(self):
        return Acuatico.remando(self)

    def cruzar_meta(self):
        print("Cruzaste la metaaaa!!!")

    def show_info(self):
        print("El vehiculo de capacidad: " + str(self.capacidad_kayak) + " con tamaño de: "
              + self.tamanio + " de color: " + self.color + " de año: " + self.anio +
              " es un kayak")


class Trajinera(Acuatico):
    def __init__(self, cantidad, nombre, color, anio, motor, capacidad):
        Vehiculo.__init__(self, color, anio)
        Acuatico.__init__(self, motor, capacidad, color, anio)
        self.cantidad = cantidad
        self.nombre = nombre

    def hacer_recorrido(self):
        print("Iniciando recorrido")

    def avanza(self):
        print("Ramando por Xochimilco...")

    def show_info(self):
        print("El vehiculo de nombre: " + str(self.nombre) + " con capacidad para: " + self.cantidad +
              " de color: " + self.color + " de año: " + self.anio + " es una traja de Xochi B): ")


class Motocicleta(Terrestre, Vehiculo):
    def __init__(self, ruedas, placa, color="blanco", anio="2022", km="16000", licencia="A2"):
        Vehiculo.__init__(self, color, anio)
        Terrestre.__init__(self, km, licencia, color, anio)
        self.ruedas = ruedas
        self.placa = placa

    def show_info(self):
        print("El vehiculo tiene: " + str(self.ruedas) + " ruedas y es de color: " + self.color +
              " y es modelo: " + self.anio + " con placas: " + self.placa + " km recorridos: " +
              self.km + " y licencia tipo: " + self.licencia + " para poder circular")

    def avanzar(self):
        return self.avanza()


class Carro(Terrestre):
    def __init__(self, numero_serie, ruedas, color, anio, km, licencia):
        Vehiculo.__init__(self, color, anio)
        Terrestre.__init__(self, km, licencia, color, anio)
        self.numero_serie = numero_serie
        self.ruedas = ruedas

    def show_info(self):
        print("El vehiculo tiene: " + str(self.ruedas) + " ruedas y es de color: " + self.color +
              " y es modelo: " + self.anio + " km recorridos: " +
              self.km + " y licencia tipo: " + self.licencia + " para poder circular")

    def avanzar(self):
        return self.avanza()


def main():
    print("Objeto moto:")
    moto = Motocicleta(2, "6L6JX")
    moto.show_info()
    moto.avanzar()
    print("Objeto carro:")
    carro = Carro("QWW234A", 4, "Azul", "2017", "66655", "A1")
    carro.show_info()
    carro.avanzar()
    print("Kayak:")
    kayak = Kayak(1, "56cm", "verde", "2015", "sin motor", "1")
    kayak.show_info()
    kayak.remando()
    kayak.cruzar_meta()
    print("Trajinera:")
    traja = Trajinera("20", "La lupita", "rojo con zul", "2000", "sin motor", "20")
    traja.show_info()
    traja.avanza()


if __name__ == '__main__':
    main()
