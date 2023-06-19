
            
        except Exception as e:
            self.dbh.log_error(e)
            raise e
        finally:
            self.dbh.disconnect();
    #entry()


if __name__ == "__main__":
    try:
        objSha = SFRun(sys.argv[3],sys.argv[4],sys.argv[2])
        objSha.entry()
    except Exception as e:
        logging.error(e,exc_info=True)
        logging.info(e)
        print(e)