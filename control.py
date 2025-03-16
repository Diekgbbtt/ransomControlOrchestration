from integrity_control import ransomCheck;
from abc import ABC, abstractmethod


class ControlClass(ABC):

    @abstractmethod
    def __init__(self, data: dict) -> None:
        return

    @abstractmethod
    def start(self) -> None:
        return

    @abstractmethod
    def stop(self) -> None:
        return


class ControlFactory:

    def __init__(self, controlName : str):
        self.control = controlName
    
    def instance_control(self, control_data : dict, cfg_data : dict) -> None:
        try:
            match self.control:
                case 'ransomCheck':
                    return ransomCheck(control_data=control_data, cfg_data=cfg_data)
                case _: # Default case to handle any unmatched control type
                    raise Exception(f"Control {self.control} not supported")
        except Exception as e:
            raise Exception(f"Error creating control instance {self.control}. \n Error : {str(e) if str(e) else e}")
        