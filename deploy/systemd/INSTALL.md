1. Copie a unit para o caminho final:
```bash
sudo cp deploy/systemd/fieldlogger.service /etc/systemd/system/fieldlogger.service
```

2. Recarregue o `systemd`:
```bash
sudo systemctl daemon-reload
```

3. Habilite para subir junto com o boot:
```bash
sudo systemctl enable fieldlogger
```

4. Inicie o servico:
```bash
sudo systemctl start fieldlogger
```

5. Validacao rapida:
```bash
sudo systemctl status fieldlogger
sudo journalctl -u fieldlogger -f
```
